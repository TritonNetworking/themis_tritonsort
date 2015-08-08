#include <algorithm>
#include <math.h>
#include <signal.h>
#include <stdlib.h>

#include "config.h"

#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30";

#include "../common/MainUtils.h"
#include "common/BufferListContainerFactory.h"
#include "common/ByteStreamBufferFactory.h"
#include "common/PartitionFile.h"
#include "common/SimpleMemoryAllocator.h"
#include "common/WriteTokenPool.h"
#include "core/AbortingDeadlockResolver.h"
#include "core/CPUAffinitySetter.h"
#include "core/DefaultAllocatorPolicy.h"
#include "core/File.h"
#include "core/Glob.h"
#include "core/IntervalStatLogger.h"
#include "core/MemoryAllocator.h"
#include "core/MemoryQuota.h"
#include "core/Params.h"
#include "core/QuotaEnforcingWorkerTracker.h"
#include "core/RedisConnection.h"
#include "core/ResourceMonitor.h"
#include "core/StatWriter.h"
#include "core/ThreadSafeVector.h"
#include "core/Timer.h"
#include "core/TrackerSet.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/WorkerTracker.h"
#include "mapreduce/common/ChunkMap.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/ListableKVPairBufferFactory.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/SampleMetadataKVPairBufferFactory.h"
#include "mapreduce/common/Utils.h"
#include "mapreduce/common/boundary/KeyPartitionerInterface.h"
#include "mapreduce/common/filter/RecordFilterMap.h"
#include "mapreduce/common/hdfs/HDFSClient.h"
#include "mapreduce/common/queueing/MapReduceWorkQueueingPolicyFactory.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"
#include "mapreduce/functions/reduce/ReduceFunctionFactory.h"
#include "mapreduce/workers/MapReduceWorkerImpls.h"

typedef BufferListContainer<ListableKVPairBuffer> KVPBContainer;

PhaseZeroOutputData* executePhaseZero(
  Params* params, IPList& peers, const StringList& intermediateDiskList,
  RecordFilterMap& recordFilterMap) {

  std::string phaseName("phase_zero");

  StatLogger phaseZeroStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  // Block until all nodes have started Themis.
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, phaseName, "", 0);
  coordinatorClient->waitOnBarrier("phase_start");

  // Configure network connections:
  // Most nodes don't receive coordinator connections.
  uint64_t numCoordinatorConnections = 0;

  bool thisNodeIsTheMerger = params->get<uint64_t>("MYPEERID") ==
    params->get<uint64_t>("MERGE_NODE_ID");

  if (thisNodeIsTheMerger) {
    // The coordinator receives coordinator connections from all peers.
    numCoordinatorConnections = peers.size();
  }

  // Coordinators send to node 0.
  IPList coordinatorSenderPeers;
  coordinatorSenderPeers.assign(peers.begin(), peers.begin() + 1);

  // All nodes receive broadcast connections from the coordinator.
  uint64_t numBroadcastConnections = 1;

  // Most nodes don't send broadcast connections to anyone.
  IPList broadcastSenderPeers;

  if (thisNodeIsTheMerger) {
    // The coordinator sends broadcast connections to all peers.
    broadcastSenderPeers.assign(peers.begin(), peers.end());
  }

  // Open TCP sockets for coordinator and broadcast connections.
  SocketArray coordinatorSenderSockets;
  SocketArray coordinatorReceiverSockets;
  openAllSockets(
    params->get<std::string>("COORDINATOR_PORT"), numCoordinatorConnections,
    *params, phaseName, "coordinator_sender", coordinatorSenderPeers, peers,
    coordinatorSenderSockets, coordinatorReceiverSockets);

  SocketArray broadcastSenderSockets;
  SocketArray broadcastReceiverSockets;
  openAllSockets(
    params->get<std::string>("RECEIVER_PORT"), numBroadcastConnections,
    *params, phaseName, "broadcast_sender", broadcastSenderPeers, peers,
    broadcastSenderSockets, broadcastReceiverSockets);

  SocketArray shuffleSenderSockets;
  SocketArray shuffleReceiverSockets;
  openAllSockets(
    params->get<std::string>("SHUFFLE_PORT"), peers.size(), *params,
    phaseName, "shuffle_sender", peers, peers, shuffleSenderSockets,
    shuffleReceiverSockets);

  // Block until all nodes have connected TCP sockets.
  coordinatorClient->waitOnBarrier("sockets_connected");
  delete coordinatorClient;

  // Set up trackers and plumb stages together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;

  WorkerTracker readerTracker(
    *params, phaseName, "reader", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker readerConverterTracker(
    *params, phaseName, "reader_converter", &queueingPolicyFactory);

  QuotaEnforcingWorkerTracker reservoirSampleMapperTracker(
    *params, phaseName, "reservoir_sample_mapper", &queueingPolicyFactory);
  WorkerTracker shuffleMapperTracker(
    *params, phaseName, "shuffle_mapper", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker shuffleSenderTracker(
    *params, phaseName, "shuffle_sender", &queueingPolicyFactory);
  WorkerTracker shuffleReceiverTracker(
    *params, phaseName, "shuffle_receiver", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker bufferCombinerTracker(
    *params, phaseName, "buffer_combiner", &queueingPolicyFactory);

  WorkerTracker mapperTracker(
    *params, phaseName, "mapper", &queueingPolicyFactory);
  WorkerTracker sorterTracker(
    *params, phaseName, "sorter", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker boundaryScannerTracker(
    *params, phaseName, "boundary_scanner", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker coordinatorSenderTracker(
    *params, phaseName, "coordinator_sender", &queueingPolicyFactory);


  WorkerTracker coordinatorReceiverTracker(
    *params, phaseName, "coordinator_receiver", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker boundaryDeciderTracker(
    *params, phaseName, "boundary_decider", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker broadcastSenderTracker(
    *params, phaseName, "broadcast_sender", &queueingPolicyFactory);

  WorkerTracker broadcastReceiverTracker(
    *params, phaseName, "broadcast_receiver", &queueingPolicyFactory);
  WorkerTracker boundaryDeserializerTracker(
    *params, phaseName, "boundary_deserializer", &queueingPolicyFactory);
  WorkerTracker boundaryHolderTracker(
    *params, phaseName, "phase_zero_output_data_holder", &queueingPolicyFactory);

  MemoryQuota readerQuota(
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.reader"));
  MemoryQuota readerConverterQuota(
    "reader_converter_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.reader_converter"));
  MemoryQuota shuffleMapperQuota(
    "shuffle_mapper_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.shuffle_mapper"));
  MemoryQuota shuffleReceiverQuota(
    "shuffle_receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.shuffle_receiver"));
  MemoryQuota sorterQuota(
    "sorter_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.sorter"));
  MemoryQuota boundaryScannerQuota(
    "boundary_scanner_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.boundary_scanner"));
  MemoryQuota coordinatorReceiverQuota(
    "coordinator_receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.coordinator_receiver"));
  MemoryQuota boundaryDeciderQuota(
    "boundary_decider_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.boundary_decider"));

  readerConverterTracker.addProducerQuota(readerQuota);
  readerConverterTracker.addConsumerQuota(readerQuota);
  reservoirSampleMapperTracker.addProducerQuota(readerConverterQuota);
  reservoirSampleMapperTracker.addConsumerQuota(readerConverterQuota);
  shuffleSenderTracker.addProducerQuota(shuffleMapperQuota);
  shuffleSenderTracker.addConsumerQuota(shuffleMapperQuota);
  bufferCombinerTracker.addProducerQuota(shuffleReceiverQuota);
  bufferCombinerTracker.addConsumerQuota(shuffleReceiverQuota);
  boundaryScannerTracker.addProducerQuota(sorterQuota);
  boundaryScannerTracker.addConsumerQuota(sorterQuota);
  coordinatorSenderTracker.addProducerQuota(boundaryScannerQuota);
  coordinatorSenderTracker.addConsumerQuota(boundaryScannerQuota);
  boundaryDeciderTracker.addProducerQuota(coordinatorReceiverQuota);
  boundaryDeciderTracker.addConsumerQuota(coordinatorReceiverQuota);
  broadcastSenderTracker.addProducerQuota(boundaryDeciderQuota);
  broadcastSenderTracker.addConsumerQuota(boundaryDeciderQuota);

  TrackerSet phaseZeroTrackers;
  phaseZeroTrackers.addTracker(&readerTracker, true);
  phaseZeroTrackers.addTracker(&readerConverterTracker);
  phaseZeroTrackers.addTracker(&reservoirSampleMapperTracker);
  phaseZeroTrackers.addTracker(&shuffleMapperTracker);
  phaseZeroTrackers.addTracker(&shuffleSenderTracker);
  phaseZeroTrackers.addTracker(&shuffleReceiverTracker, true);
  phaseZeroTrackers.addTracker(&bufferCombinerTracker);
  phaseZeroTrackers.addTracker(&mapperTracker);
  phaseZeroTrackers.addTracker(&sorterTracker);
  phaseZeroTrackers.addTracker(&boundaryScannerTracker);
  phaseZeroTrackers.addTracker(&coordinatorSenderTracker);
  phaseZeroTrackers.addTracker(&broadcastSenderTracker);
  phaseZeroTrackers.addTracker(&broadcastReceiverTracker, true);
  phaseZeroTrackers.addTracker(&coordinatorReceiverTracker, true);
  phaseZeroTrackers.addTracker(&boundaryDeciderTracker);
  phaseZeroTrackers.addTracker(&boundaryDeserializerTracker);
  phaseZeroTrackers.addTracker(&boundaryHolderTracker);

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&readerConverterTracker);
  readerConverterTracker.addDownstreamTracker(&reservoirSampleMapperTracker);
  reservoirSampleMapperTracker.addDownstreamTracker(&shuffleMapperTracker);
  shuffleMapperTracker.addDownstreamTracker(&shuffleSenderTracker);
  shuffleReceiverTracker.isSourceTracker();
  shuffleReceiverTracker.addDownstreamTracker(&bufferCombinerTracker);
  bufferCombinerTracker.addDownstreamTracker(&mapperTracker);
  mapperTracker.addDownstreamTracker(&sorterTracker);
  sorterTracker.addDownstreamTracker(&boundaryScannerTracker);
  boundaryScannerTracker.addDownstreamTracker(&coordinatorSenderTracker);
  coordinatorReceiverTracker.isSourceTracker();
  coordinatorReceiverTracker.addDownstreamTracker(&boundaryDeciderTracker);
  broadcastReceiverTracker.isSourceTracker();
  broadcastReceiverTracker.addDownstreamTracker(&boundaryDeserializerTracker);
  boundaryDeciderTracker.addDownstreamTracker(&broadcastSenderTracker);
  boundaryDeserializerTracker.addDownstreamTracker(&boundaryHolderTracker);

  // Make sure the reader tracker gets its read requests.
  loadReadRequests(readerTracker, *params, phaseName);

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;

  FilenameToStreamIDMap inputFilenameToStreamIDMap;

  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);
  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");

  readerConverterTracker.setFactory(
    &workerFactory, "mapreduce", "byte_stream_converter");

  mapperTracker.setFactory(&workerFactory, "mapreduce", "mapper");

  sorterTracker.setFactory(&workerFactory, "mapreduce", "sorter");

  boundaryScannerTracker.setFactory(
    &workerFactory, "mapreduce", "boundary_scanner");
  coordinatorSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");

  coordinatorReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  broadcastReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  boundaryDeciderTracker.setFactory(
    &workerFactory, "mapreduce", "boundary_decider");

  boundaryDeserializerTracker.setFactory(
    &workerFactory, "mapreduce", "boundary_deserializer");

  broadcastSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");

  boundaryHolderTracker.setFactory(
    &workerFactory, "mapreduce", "phase_zero_output_data_holder");

  reservoirSampleMapperTracker.setFactory(
    &workerFactory, "mapreduce", "mapper");

  shuffleMapperTracker.setFactory(
    &workerFactory, "mapreduce", "mapper");

  shuffleSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");

  shuffleReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  bufferCombinerTracker.setFactory(
    &workerFactory, "mapreduce", "buffer_combiner");

  // Inject dependencies into factories
  workerFactory.addDependency(
    "coordinator_sender", "sockets", &coordinatorSenderSockets);
  workerFactory.addDependency(
    "coordinator_receiver", "sockets", &coordinatorReceiverSockets);

  workerFactory.addDependency(
    "broadcast_sender", "sockets", &broadcastSenderSockets);
  workerFactory.addDependency(
    "broadcast_receiver", "sockets", &broadcastReceiverSockets);

  std::string readerConverterStageName("reader_converter");
  workerFactory.addDependency("reader", "filename_to_stream_id_map",
                              &inputFilenameToStreamIDMap);
  workerFactory.addDependency("reader", "converter_stage_name",
                              &readerConverterStageName);
  workerFactory.addDependency("reader_converter", "filename_to_stream_id_map",
                              &inputFilenameToStreamIDMap);

  PartitionFunctionMap partitionFunctionMap(*params, phaseName);
  workerFactory.addDependency("partition_function_map", &partitionFunctionMap);
  workerFactory.addDependency("record_filter_map", &recordFilterMap);

  workerFactory.addDependency(
    "shuffle_sender", "sockets", &shuffleSenderSockets);
  workerFactory.addDependency(
    "shuffle_receiver", "sockets", &shuffleReceiverSockets);

  bool shuffle = true;
  workerFactory.addDependency("shuffle_mapper", "shuffle", &shuffle);

  bool reservoirSample = true;
  workerFactory.addDependency(
    "reservoir_sample_mapper", "reservoir_sample", &reservoirSample);

  // Create workers
  phaseZeroTrackers.createWorkers();

  // Start everything going
  Timer phaseZeroTimer;

  phaseZeroTimer.start();

  phaseZeroTrackers.spawn();

  // Wait for everything to finish
  phaseZeroTrackers.waitForWorkersToFinish();

  // Log phase start and end time
  phaseZeroTimer.stop();
  phaseZeroStatLogger.logDatum("phase_runtime", phaseZeroTimer);

  // Retrieve KeyPartitionerInterface from partition_holder stage

  PhaseZeroOutputData* outputData = NULL;
  ItemHolder<PhaseZeroOutputData>* outputDataHolder = NULL;

  ThreadSafeVector<BaseWorker*>& outputDataHolders =
    boundaryHolderTracker.getWorkers();

  ABORT_IF(outputDataHolders.size() == 0, "Not enough boundary holders to grab "
           "a worker's boundary list");
  outputDataHolder = dynamic_cast<ItemHolder<PhaseZeroOutputData>*>(
    outputDataHolders[0]);

  const std::list<PhaseZeroOutputData*>& heldItems =
    outputDataHolder->getHeldItems();
  ABORT_IF(heldItems.size() != 1, "Should hold %llu boundary lists at the "
           "end of the phase, but held %llu", 1, heldItems.size());
  outputData = heldItems.back();

  delete memoryAllocator;

  // Destroy workers
  phaseZeroTrackers.destroyWorkers();

  // Unlike in the PartitionTrie version of phase 0, we do not need to make a
  // copy of the final output here because PhaseZeroOutputDatas are not
  // resources and therefore don't come from pools. We will return the pointer
  // directly.
  return outputData;
}

void executePhaseOne(
  Params* params, IPList& peers, const StringList& intermediateDiskList,
  RecordFilterMap& recordFilterMap) {

  std::string phaseName("phase_one");

  StatLogger phaseOneStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  // Block until all nodes have started Themis.
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, phaseName, "", 0);
  coordinatorClient->waitOnBarrier("phase_start");

  // Open all-to-all TCP sockets.
  SocketArray senderSockets;
  SocketArray receiverSockets;
  openAllSockets(
    params->get<std::string>("RECEIVER_PORT"), peers.size(), *params, phaseName,
    "sender", peers, peers, senderSockets, receiverSockets);

  // Block until all nodes have connected TCP sockets.
  coordinatorClient->waitOnBarrier("sockets_connected");
  delete coordinatorClient;

  // Set up trackers and plumb workers together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;

  // Selectively enable/disable chainer and coalescer based on params.
  bool useWriteChaining = params->get<bool>("USE_WRITE_CHAINING");

  WorkerTracker readerTracker(
    *params, phaseName, "reader", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker readerConverterTracker(
    *params, phaseName, "reader_converter", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker mapperTracker(
    *params, phaseName, "mapper", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker senderTracker(
    *params, phaseName, "sender", &queueingPolicyFactory);

  WorkerTracker receiverTracker(
    *params, phaseName, "receiver", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker tupleDemuxTracker(
    *params, phaseName, "demux", &queueingPolicyFactory);
  WorkerTracker chainerTracker(
    *params, phaseName, "chainer", &queueingPolicyFactory);
  WorkerTracker coalescerTracker(
    *params, phaseName, "coalescer", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker writerTracker(
    *params, phaseName, "writer", &queueingPolicyFactory);

  MemoryQuota readerQuota(
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reader"));
  MemoryQuota readerConverterQuota(
    "reader_converter_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reader_converter"));
  MemoryQuota mapperQuota(
    "mapper_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.mapper"));
  MemoryQuota receiverQuota(
    "receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.receiver"));

  readerConverterTracker.addProducerQuota(readerQuota);
  readerConverterTracker.addConsumerQuota(readerQuota);
  mapperTracker.addProducerQuota(readerConverterQuota);
  mapperTracker.addConsumerQuota(readerConverterQuota);
  senderTracker.addProducerQuota(mapperQuota);
  senderTracker.addConsumerQuota(mapperQuota);
  tupleDemuxTracker.addProducerQuota(receiverQuota);
  writerTracker.addConsumerQuota(receiverQuota);

  TrackerSet phaseOneTrackers;
  phaseOneTrackers.addTracker(&readerTracker, true);
  phaseOneTrackers.addTracker(&readerConverterTracker);
  phaseOneTrackers.addTracker(&mapperTracker);
  phaseOneTrackers.addTracker(&senderTracker);
  phaseOneTrackers.addTracker(&receiverTracker, true);
  phaseOneTrackers.addTracker(&tupleDemuxTracker);
  if (useWriteChaining) {
    phaseOneTrackers.addTracker(&chainerTracker);
    phaseOneTrackers.addTracker(&coalescerTracker);
  }
  phaseOneTrackers.addTracker(&writerTracker);

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&readerConverterTracker);
  readerConverterTracker.addDownstreamTracker(&mapperTracker);
  mapperTracker.addDownstreamTracker(&senderTracker);

  receiverTracker.isSourceTracker();
  receiverTracker.addDownstreamTracker(&tupleDemuxTracker);
  if (useWriteChaining) {
    // Demux -> Chainer -> Coalescer -> Writer
    tupleDemuxTracker.addDownstreamTracker(&chainerTracker);
    chainerTracker.addDownstreamTracker(&coalescerTracker);
    coalescerTracker.addDownstreamTracker(&writerTracker);
  } else {
    // Demux -> Writer
    tupleDemuxTracker.addDownstreamTracker(&writerTracker);
  }

  // Make sure the reader tracker gets its read requests.
  loadReadRequests(readerTracker, *params, phaseName);

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;

  FilenameToStreamIDMap inputFilenameToStreamIDMap;

  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);

  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");
  readerConverterTracker.setFactory(
    &workerFactory, "mapreduce", "byte_stream_converter");
  mapperTracker.setFactory(&workerFactory, "mapreduce", "mapper");
  senderTracker.setFactory(&workerFactory, "mapreduce", "kv_pair_buf_sender");
  receiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");
  tupleDemuxTracker.setFactory(&workerFactory, "mapreduce", "tuple_demux");
  chainerTracker.setFactory(&workerFactory, "mapreduce", "chainer");
  coalescerTracker.setFactory(&workerFactory, "mapreduce", "coalescer");
  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  // Set up resources
  uint64_t numIntermediateDisks = intermediateDiskList.size();
  ABORT_IF(numIntermediateDisks == 0,
           "Must specify more than one intermediate directory");

  WriteTokenPool writeTokenPool(
    params->get<uint64_t>("TOKENS_PER_WRITER"), numIntermediateDisks);

  // Inject dependencies into factories
  std::string chainerStageName("chainer");
  std::string converterStageName("reader_converter");
  std::string coalescerStageName("coalescer");
  std::string writerStageName("writer");

  workerFactory.addDependency("sender", "sockets", &senderSockets);
  workerFactory.addDependency("receiver", "sockets", &receiverSockets);

  workerFactory.addDependency(
    "reader", "filename_to_stream_id_map", &inputFilenameToStreamIDMap);
  workerFactory.addDependency(
    "reader", "converter_stage_name", &converterStageName);
  workerFactory.addDependency(
    "reader_converter", "filename_to_stream_id_map",
    &inputFilenameToStreamIDMap);
  workerFactory.addDependency("write_token_pool", &writeTokenPool);

  if (useWriteChaining) {
    workerFactory.addDependency(
      "demux", "downstream_stage_name", &chainerStageName);
  } else {
    workerFactory.addDependency(
      "demux", "downstream_stage_name", &writerStageName);
  }

  workerFactory.addDependency(
    "chainer", "coalescer_stage_name", &coalescerStageName);

  PartitionFunctionMap partitionFunctionMap(*params, phaseName);
  workerFactory.addDependency("partition_function_map", &partitionFunctionMap);
  workerFactory.addDependency("record_filter_map", &recordFilterMap);

  workerFactory.addDependency(
    "output_disk_list", const_cast<StringList*>(&intermediateDiskList));

  // Create workers
  phaseOneTrackers.createWorkers();

  StatusPrinter::flush();

  // Start everything going
  Timer phaseOneTimer;
  phaseOneTimer.start();

  // kick off the source stages
  phaseOneTrackers.spawn();

  phaseOneTrackers.waitForWorkersToFinish();

  StatusPrinter::add("All phase 1 workers are finished");
  StatusPrinter::flush();

  // All workers have finished

  phaseOneTimer.stop();

  phaseOneStatLogger.logDatum("phase_runtime", phaseOneTimer);

  delete memoryAllocator;

  // Destroy workers
  phaseOneTrackers.destroyWorkers();

  StatusPrinter::add("Phase 1 complete");
  StatusPrinter::flush();
}

void executePhaseTwo(
  Params* params, IPList& peers, const StringList& intermediateDiskList,
  const StringList& outputDiskList) {

  std::string phaseName = "phase_two";

  StatLogger phaseTwoStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  CoordinatorClientInterface* barrierCoordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, phaseName, "", 0);

  uint64_t replicationLevel = params->get<uint64_t>("OUTPUT_REPLICATION_LEVEL");
  if (replicationLevel > 1) {
    // If we're using replication, we need all nodes to synchronize before and
    // after sockets are connected.
    barrierCoordinatorClient->waitOnBarrier("phase_start");
  }

  uint64_t localNodeID = params->get<uint64_t>("MYPEERID");
  uint64_t numNodes = params->get<uint64_t>("NUM_PEERS");

  IPList replicaSenderPeers;
  for (uint64_t i = 1; i < numNodes; i++) {
    // Replicate to consecutive nodes.
    replicaSenderPeers.push_back(peers.at((localNodeID + i) % numNodes));
  }

  // Open TCP connections to replica nodes.
  SocketArray replicaSenderSockets;
  SocketArray replicaReceiverSockets;
  if (replicationLevel > 1) {
    openAllSockets(
      params->get<std::string>("REPLICA_RECEIVER_PORT"), numNodes - 1,
      *params, phaseName, "replica_sender", replicaSenderPeers, peers,
      replicaSenderSockets, replicaReceiverSockets);

    barrierCoordinatorClient->waitOnBarrier("sockets_connected");
  }
  delete barrierCoordinatorClient;


  // Set up trackers and plumb stages together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;

  WorkerTracker readerTracker(
    *params, phaseName, "reader", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker converterTracker(
    *params, phaseName, "reader_converter", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker sorterTracker(
    *params, phaseName, "sorter", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker reducerTracker(
    *params, phaseName, "reducer", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker writerTracker(
    *params, phaseName, "writer", &queueingPolicyFactory);

  QuotaEnforcingWorkerTracker replicaSenderTracker(
    *params, phaseName, "replica_sender", &queueingPolicyFactory);
  WorkerTracker replicaReceiverTracker(
    *params, phaseName, "replica_receiver", &queueingPolicyFactory);

  MemoryQuota readerQuota(
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_two.reader"));
  MemoryQuota converterQuota(
    "converter_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_two.reader_converter"));
  MemoryQuota sorterQuota(
    "sorter_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_two.sorter"));
  MemoryQuota reducerQuota(
    "reducer_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_two.reducer"));

  MemoryQuota reducerReplicaQuota(
    "reducer_replica_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_two.reducer_replica"));

  bool useConverter = params->contains("FORMAT_READER.phase_two");

  if (useConverter) {
    converterTracker.addProducerQuota(readerQuota);
    converterTracker.addConsumerQuota(readerQuota);
    sorterTracker.addProducerQuota(converterQuota);
    sorterTracker.addConsumerQuota(converterQuota);
  } else {
    sorterTracker.addProducerQuota(readerQuota);
    sorterTracker.addConsumerQuota(readerQuota);
  }
  reducerTracker.addProducerQuota(sorterQuota);
  reducerTracker.addConsumerQuota(sorterQuota);
  writerTracker.addProducerQuota(reducerQuota);
  writerTracker.addConsumerQuota(reducerQuota);


  if (replicationLevel > 1) {
    replicaSenderTracker.addProducerQuota(reducerReplicaQuota);
    replicaSenderTracker.addConsumerQuota(reducerReplicaQuota);
  }

  TrackerSet phaseTwoTrackers;
  phaseTwoTrackers.addTracker(&readerTracker, true);
  if (useConverter) {
    phaseTwoTrackers.addTracker(&converterTracker);
  }
  phaseTwoTrackers.addTracker(&sorterTracker);
  phaseTwoTrackers.addTracker(&reducerTracker);
  phaseTwoTrackers.addTracker(&writerTracker);

  if (replicationLevel > 1) {
    phaseTwoTrackers.addTracker(&replicaSenderTracker);
    phaseTwoTrackers.addTracker(&replicaReceiverTracker, true);
  }

  readerTracker.isSourceTracker();
  if (useConverter) {
    readerTracker.addDownstreamTracker(&converterTracker);
    converterTracker.addDownstreamTracker(&sorterTracker);
  } else {
    readerTracker.addDownstreamTracker(&sorterTracker);
  }
  sorterTracker.addDownstreamTracker(&reducerTracker);
  reducerTracker.addDownstreamTracker(&writerTracker);

  if (replicationLevel > 1) {
    reducerTracker.addDownstreamTracker(&replicaSenderTracker);
    replicaReceiverTracker.isSourceTracker();
    replicaReceiverTracker.addDownstreamTracker(&writerTracker);
  }

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;
  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);
  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");
  converterTracker.setFactory(
    &workerFactory, "mapreduce", "byte_stream_converter");
  sorterTracker.setFactory(&workerFactory, "mapreduce", "sorter");
  reducerTracker.setFactory(&workerFactory, "mapreduce", "reducer");
  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  replicaSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");
  replicaReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  // Add intermediate files to the appropriate reader for each job
  std::list<uint64_t> jobIDList;
  parseCommaDelimitedList< uint64_t, std::list<uint64_t> >(
    jobIDList, params->get<std::string>("JOB_IDS"));

  // We need to get the phase one output directory for each job.
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, "phase_one", "", 0);
  for (std::list<uint64_t>::iterator jobIter = jobIDList.begin();
         jobIter != jobIDList.end(); jobIter++) {
    const URL& outputDirectory =
      coordinatorClient->getOutputDirectory(*jobIter);
    ABORT_IF(outputDirectory.scheme() != "local",
             "Intermediate files can only be read from local disk");

    // Add each disk to the correct reader.
    uint64_t diskID = 0;
    for (StringList::const_iterator diskIter = intermediateDiskList.begin();
         diskIter != intermediateDiskList.end(); diskIter++) {
      Glob intermediateGlob(
        *diskIter + outputDirectory.path() + "/*.partition");

      const StringList& intermediateFiles = intermediateGlob.getFiles();
      StringVector fileVector;
      for (StringList::const_iterator fileIter = intermediateFiles.begin();
           fileIter != intermediateFiles.end(); fileIter++) {
        fileVector.push_back(*fileIter);
      }
      std::random_shuffle(fileVector.begin(), fileVector.end() );

      for (StringVector::iterator fileIter = fileVector.begin();
           fileIter != fileVector.end(); fileIter++) {

        PartitionFile file(*fileIter);
        ReadRequest* request = new ReadRequest(*fileIter, diskID);
        request->jobIDs.insert(file.getJobID());
        readerTracker.addWorkUnit(request);
      }

      diskID++;
    }
  }

  delete coordinatorClient;

  FilenameToStreamIDMap intermediateFilenameToStreamIDMap;
  if (useConverter) {
    workerFactory.addDependency("reader", "filename_to_stream_id_map",
                                &intermediateFilenameToStreamIDMap);
    workerFactory.addDependency("reader_converter", "filename_to_stream_id_map",
                                &intermediateFilenameToStreamIDMap);
  }

  workerFactory.addDependency(
    "output_disk_list", const_cast<StringList*>(&outputDiskList));

  workerFactory.addDependency(
    "replica_sender", "sockets", &replicaSenderSockets);
  workerFactory.addDependency(
    "replica_receiver", "sockets", &replicaReceiverSockets);

  phaseTwoTrackers.createWorkers();

  Timer phaseTwoTimer;
  phaseTwoTimer.start();

  // Start everything going
  phaseTwoTrackers.spawn();

  phaseTwoTrackers.waitForWorkersToFinish();

  phaseTwoTimer.stop();

  phaseTwoStatLogger.logDatum("phase_runtime", phaseTwoTimer);

  delete memoryAllocator;

  phaseTwoTrackers.destroyWorkers();

  StatusPrinter::add("Phase 2 complete");
  StatusPrinter::flush();
}

void executePhaseThree(
  Params* params, IPList& peers, const StringList& intermediateDiskList,
  const StringList& outputDiskList) {
  // Phase Three is responsible for performing an external merge sort on the
  // partitions that were too large to process in phase two. It operates in two
  // subphases.
  //  splitsort: Split and Sort
  //    Split the large partition into manageable chunks, sort each chunk, and
  //    write sorted chunk files back to intermediate disks.
  //  mergereduce: Merge and Reduce
  //    Stream sorted chunks from disk into an in-memory merger that constructs
  //    a streaming representation of the sorted partition.  This sorted
  //    partition is directed, one buffer at a time, to a reducer that is
  //    configured to expect multiple buffers per partition (since we can't fit
  //    the entire partition in memory). Finally, the reduced partition is
  //    written back to output disks.

  std::string phaseName = "phase_three";

  StatLogger phaseThreeStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  // The first part of phase three reads from intermediate disks and writes back
  // to intermediate disks, so configure the chunk map accordingly.
  uint64_t numSplitSortDisks = intermediateDiskList.size();
  ChunkMap chunkMap(numSplitSortDisks);

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;
  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);

  // Parse job IDs.
  std::list<uint64_t> jobIDList;
  parseCommaDelimitedList< uint64_t, std::list<uint64_t> >(
    jobIDList, params->get<std::string>("JOB_IDS"));

  // Large partition files and smaller chunk files reside in the intermediate
  // file directory, which is phase one's output directory.
  // in the phase one output directory, so get that.
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, "phase_one", "", 0);

  // ===============//
  // Split and Sort //
  // ============== //
  params->add<uint64_t>(
    "NUM_OUTPUT_DISKS.phase_three", numSplitSortDisks);

  MapReduceWorkQueueingPolicyFactory splitSortQueueingPolicyFactory;
  splitSortQueueingPolicyFactory.setChunkMap(&chunkMap);

  WorkerTracker splitSortReaderTracker(
    *params, phaseName, "splitsort_reader", &splitSortQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker splitSortConverterTracker(
    *params, phaseName, "splitsort_reader_converter",
    &splitSortQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker sorterTracker(
    *params, phaseName, "sorter", &splitSortQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker splitSortWriterTracker(
    *params, phaseName, "splitsort_writer", &splitSortQueueingPolicyFactory);

  MemoryQuota splitSortReaderQuota(
    "splitsort_reader_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.splitsort_reader"));
  MemoryQuota splitSortConverterQuota(
    "splitsort_converter_quota",
    params->get<uint64_t>(
      "MEMORY_QUOTAS.phase_three.splitsort_reader_converter"));
  MemoryQuota sorterQuota(
    "sorter_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.sorter"));

  splitSortConverterTracker.addProducerQuota(splitSortReaderQuota);
  splitSortConverterTracker.addConsumerQuota(splitSortReaderQuota);
  sorterTracker.addProducerQuota(splitSortConverterQuota);
  sorterTracker.addConsumerQuota(splitSortConverterQuota);
  splitSortWriterTracker.addProducerQuota(sorterQuota);
  splitSortWriterTracker.addConsumerQuota(sorterQuota);

  TrackerSet splitSortTrackers;
  splitSortTrackers.addTracker(&splitSortReaderTracker, true);
  splitSortTrackers.addTracker(&splitSortConverterTracker);
  splitSortTrackers.addTracker(&sorterTracker);
  splitSortTrackers.addTracker(&splitSortWriterTracker);

  splitSortReaderTracker.isSourceTracker();
  splitSortReaderTracker.addDownstreamTracker(&splitSortConverterTracker);
  splitSortConverterTracker.addDownstreamTracker(&sorterTracker);
  sorterTracker.addDownstreamTracker(&splitSortWriterTracker);

  WorkerFactory splitSortWorkerFactory(
    *params, cpuAffinitySetter, *memoryAllocator);
  splitSortWorkerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);

  splitSortReaderTracker.setFactory(
    &splitSortWorkerFactory, "mapreduce", "reader");
  splitSortConverterTracker.setFactory(
    &splitSortWorkerFactory, "mapreduce", "byte_stream_converter");
  sorterTracker.setFactory(&splitSortWorkerFactory, "mapreduce", "sorter");
  splitSortWriterTracker.setFactory(
    &splitSortWorkerFactory, "mapreduce", "writer");

  // Add dependencies to factory.
  FilenameToStreamIDMap largePartitionFilenameToStreamIDMap;
  splitSortWorkerFactory.addDependency(
    "splitsort_reader", "filename_to_stream_id_map",
    &largePartitionFilenameToStreamIDMap);
  splitSortWorkerFactory.addDependency(
    "splitsort_reader_converter", "filename_to_stream_id_map",
    &largePartitionFilenameToStreamIDMap);

  splitSortWorkerFactory.addDependency(
    "output_disk_list", const_cast<StringList*>(&intermediateDiskList));

  splitSortWorkerFactory.addDependency(
    "chunk_map", &chunkMap);

  for (std::list<uint64_t>::iterator jobIter = jobIDList.begin();
         jobIter != jobIDList.end(); jobIter++) {
    const URL& outputDirectory =
      coordinatorClient->getOutputDirectory(*jobIter);
    ABORT_IF(outputDirectory.scheme() != "local",
             "Intermediate files can only be read from local disk");

    // Check each intermediate disk for large files.
    uint64_t diskID = 0;
    for (StringList::const_iterator diskIter = intermediateDiskList.begin();
         diskIter != intermediateDiskList.end(); diskIter++) {
      Glob intermediateGlob(
        *diskIter + outputDirectory.path() + "/*.partition.large");

      const StringList& intermediateFiles = intermediateGlob.getFiles();

      for (StringList::const_iterator fileIter = intermediateFiles.begin();
           fileIter != intermediateFiles.end(); fileIter++) {

        PartitionFile file(*fileIter);
        ReadRequest* request = new ReadRequest(*fileIter, diskID);
        request->jobIDs.insert(file.getJobID());

        splitSortReaderTracker.addWorkUnit(request);

      }

      diskID++;
    }
  }

  splitSortTrackers.createWorkers();

  Timer phaseThreeTimer;
  phaseThreeTimer.start();

  Timer splitSortTimer;
  splitSortTimer.start();

  // Start everything going
  splitSortTrackers.spawn();

  splitSortTrackers.waitForWorkersToFinish();

  splitSortTimer.stop();
  phaseThreeStatLogger.logDatum("splitsort_runtime", splitSortTimer);

  splitSortTrackers.destroyWorkers();

  // =================//
  // Merge and Reduce //
  // ================ //

  CoordinatorClientInterface* barrierCoordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(*params, phaseName, "", 0);

  uint64_t replicationLevel = params->get<uint64_t>("OUTPUT_REPLICATION_LEVEL");
  if (replicationLevel > 1) {
    // If we're using replication, we need all nodes to synchronize before and
    // after sockets are connected.
    barrierCoordinatorClient->waitOnBarrier("phase_start");
  }

  uint64_t localNodeID = params->get<uint64_t>("MYPEERID");
  uint64_t numNodes = params->get<uint64_t>("NUM_PEERS");

  IPList replicaSenderPeers;
  for (uint64_t i = 1; i < numNodes; i++) {
    // Replicate to consecutive nodes.
    replicaSenderPeers.push_back(peers.at((localNodeID + i) % numNodes));
  }

  // Open TCP connections to replica nodes.
  SocketArray replicaSenderSockets;
  SocketArray replicaReceiverSockets;
  if (replicationLevel > 1) {
    openAllSockets(
      params->get<std::string>("REPLICA_RECEIVER_PORT"), numNodes - 1,
      *params, phaseName, "replica_sender", replicaSenderPeers, peers,
      replicaSenderSockets, replicaReceiverSockets);

    barrierCoordinatorClient->waitOnBarrier("sockets_connected");
  }
  delete barrierCoordinatorClient;


  // This time we'll write to the output disks.
  params->add<uint64_t>(
    "NUM_OUTPUT_DISKS.phase_three", outputDiskList.size());
  // SplitSort writes KeyValuePairs with headers, so read with the appropriate
  // format.
  params->add<std::string>(
    "FORMAT_READER.phase_three", "KVPairFormatReader");
  // MergeReduce requires the readers to read all files in parallel, so force a
  // LibAIOReader implementation.
  params->add<std::string>(
    "WORKER_IMPLS.phase_three.mergereduce_reader", "LibAIOReader");
  // Write merged partitions back to the phase two output directory.
  params->add<bool>("FORCE_PHASE_TWO_OUTPUT_DIR", true);

  MapReduceWorkQueueingPolicyFactory mergeReduceQueueingPolicyFactory;
  mergeReduceQueueingPolicyFactory.setChunkMap(&chunkMap);

  WorkerTracker mergeReduceReaderTracker(
    *params, phaseName, "mergereduce_reader",
    &mergeReduceQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker mergeReduceConverterTracker(
    *params, phaseName, "mergereduce_reader_converter",
    &mergeReduceQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker mergerTracker(
    *params, phaseName, "merger", &mergeReduceQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker reducerTracker(
    *params, phaseName, "reducer", &mergeReduceQueueingPolicyFactory);
  QuotaEnforcingWorkerTracker mergeReduceWriterTracker(
    *params, phaseName, "mergereduce_writer",
    &mergeReduceQueueingPolicyFactory);

  QuotaEnforcingWorkerTracker replicaSenderTracker(
    *params, phaseName, "replica_sender", &mergeReduceQueueingPolicyFactory);
  WorkerTracker replicaReceiverTracker(
    *params, phaseName, "replica_receiver", &mergeReduceQueueingPolicyFactory);

  MemoryQuota mergeReduceReaderQuota(
    "mergereduce_reader_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.mergereduce_reader"));
  MemoryQuota mergeReduceConverterQuota(
    "mergereduce_converter_quota",
    params->get<uint64_t>(
      "MEMORY_QUOTAS.phase_three.mergereduce_reader_converter"));
  MemoryQuota mergerQuota(
    "merger_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.merger"));
  MemoryQuota reducerQuota(
    "reducer_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.reducer"));

  MemoryQuota reducerReplicaQuota(
    "reducer_replica_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_three.reducer_replica"));

  mergeReduceConverterTracker.addProducerQuota(mergeReduceReaderQuota);
  mergeReduceConverterTracker.addConsumerQuota(mergeReduceReaderQuota);
  mergerTracker.addProducerQuota(mergeReduceConverterQuota);
  mergerTracker.addConsumerQuota(mergeReduceConverterQuota);
  reducerTracker.addProducerQuota(mergerQuota);
  reducerTracker.addConsumerQuota(mergerQuota);
  mergeReduceWriterTracker.addProducerQuota(reducerQuota);
  mergeReduceWriterTracker.addConsumerQuota(reducerQuota);

  if (replicationLevel > 1) {
    replicaSenderTracker.addProducerQuota(reducerReplicaQuota);
    replicaSenderTracker.addConsumerQuota(reducerReplicaQuota);
  }

  TrackerSet mergeReduceTrackers;
  mergeReduceTrackers.addTracker(&mergeReduceReaderTracker, true);
  mergeReduceTrackers.addTracker(&mergeReduceConverterTracker);
  mergeReduceTrackers.addTracker(&mergerTracker);
  mergeReduceTrackers.addTracker(&reducerTracker);
  mergeReduceTrackers.addTracker(&mergeReduceWriterTracker);

  if (replicationLevel > 1) {
    mergeReduceTrackers.addTracker(&replicaSenderTracker);
    mergeReduceTrackers.addTracker(&replicaReceiverTracker, true);
  }

  mergeReduceReaderTracker.isSourceTracker();
  mergeReduceReaderTracker.addDownstreamTracker(&mergeReduceConverterTracker);
  mergeReduceConverterTracker.addDownstreamTracker(&mergerTracker);
  mergerTracker.addDownstreamTracker(&reducerTracker);
  reducerTracker.addDownstreamTracker(&mergeReduceWriterTracker);

  if (replicationLevel > 1) {
    reducerTracker.addDownstreamTracker(&replicaSenderTracker);
    replicaReceiverTracker.isSourceTracker();
    replicaReceiverTracker.addDownstreamTracker(&mergeReduceWriterTracker);
  }

  WorkerFactory mergeReduceWorkerFactory(
    *params, cpuAffinitySetter, *memoryAllocator);
  mergeReduceWorkerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);

  mergeReduceReaderTracker.setFactory(
    &mergeReduceWorkerFactory, "mapreduce", "reader");
  mergeReduceConverterTracker.setFactory(
    &mergeReduceWorkerFactory, "mapreduce", "byte_stream_converter");
  mergerTracker.setFactory(&mergeReduceWorkerFactory, "mapreduce", "merger");
  reducerTracker.setFactory(&mergeReduceWorkerFactory, "mapreduce", "reducer");
  mergeReduceWriterTracker.setFactory(
    &mergeReduceWorkerFactory, "mapreduce", "writer");

  replicaSenderTracker.setFactory(
    &mergeReduceWorkerFactory, "mapreduce", "kv_pair_buf_sender");
  replicaReceiverTracker.setFactory(
    &mergeReduceWorkerFactory, "mapreduce", "kv_pair_buf_receiver");

  // Add dependencies to factory.
  FilenameToStreamIDMap chunkFilenameToStreamIDMap;
  mergeReduceWorkerFactory.addDependency(
    "mergereduce_reader", "filename_to_stream_id_map",
    &chunkFilenameToStreamIDMap);
  mergeReduceWorkerFactory.addDependency(
    "mergereduce_reader_converter", "filename_to_stream_id_map",
    &chunkFilenameToStreamIDMap);

  // Write to output disks.
  mergeReduceWorkerFactory.addDependency(
    "output_disk_list", const_cast<StringList*>(&outputDiskList));

  mergeReduceWorkerFactory.addDependency(
    "chunk_map", &chunkMap);

  mergeReduceWorkerFactory.addDependency(
    "replica_sender", "sockets", &replicaSenderSockets);
  mergeReduceWorkerFactory.addDependency(
    "replica_receiver", "sockets", &replicaReceiverSockets);

  // Count the number of chunks.
  uint64_t numChunks = 0;
  const ChunkMap::DiskMap& diskMap = chunkMap.getDiskMap();
  for (ChunkMap::DiskMap::const_iterator iter = diskMap.begin();
       iter != diskMap.end(); iter++) {
    numChunks += (iter->second).size();
  }

  // Split the merge memory equally across chunks.
  uint64_t mergeMemory = params->get<uint64_t>("MERGE_MEMORY");
  uint64_t readBufferSize = params->get<uint64_t>(
    "DEFAULT_BUFFER_SIZE.phase_three.mergereduce_reader");
  uint64_t numBuffers = mergeMemory / readBufferSize;
  uint64_t buffersPerChunk = 0;
  if (numChunks > 0) {
    buffersPerChunk = numBuffers / numChunks;
  }

  WriteTokenPool tokenPool(buffersPerChunk, numChunks);

  mergeReduceWorkerFactory.addDependency("read_token_pool", &tokenPool);

  for (std::list<uint64_t>::iterator jobIter = jobIDList.begin();
         jobIter != jobIDList.end(); jobIter++) {
    const URL& outputDirectory =
      coordinatorClient->getOutputDirectory(*jobIter);
    ABORT_IF(outputDirectory.scheme() != "local",
             "Intermediate files can only be read from local disk");

    // Check each intermediate disk for large files.
    uint64_t diskID = 0;
    for (StringList::const_iterator diskIter = intermediateDiskList.begin();
         diskIter != intermediateDiskList.end(); diskIter++) {
      Glob intermediateGlob(
        *diskIter + outputDirectory.path() + "/*.partition.chunk_*");

      const StringList& intermediateFiles = intermediateGlob.getFiles();

      for (StringList::const_iterator fileIter = intermediateFiles.begin();
           fileIter != intermediateFiles.end(); fileIter++) {

        PartitionFile file(*fileIter);
        ReadRequest* request = new ReadRequest(*fileIter, diskID);
        request->jobIDs.insert(file.getJobID());

        mergeReduceReaderTracker.addWorkUnit(request);
      }

      diskID++;
    }
  }

  // Force asynchronous readers to read all files.
  uint64_t asynchronousIODepth = numChunks;
  if (asynchronousIODepth == 0) {
    // AsynchronousReader will throw errors if instantiated with 0 depth.
    asynchronousIODepth = 1;
  }
  params->add<uint64_t>(
    "ASYNCHRONOUS_IO_DEPTH.phase_three.mergereduce_reader",
    asynchronousIODepth);

  mergeReduceTrackers.createWorkers();

  Timer mergeReduceTimer;
  mergeReduceTimer.start();

  // Start everything going
  mergeReduceTrackers.spawn();

  mergeReduceTrackers.waitForWorkersToFinish();

  mergeReduceTimer.stop();
  phaseThreeStatLogger.logDatum("mergereduce_runtime", mergeReduceTimer);

  mergeReduceTrackers.destroyWorkers();

  phaseThreeStatLogger.logDatum("phase_runtime", phaseThreeTimer);

  delete coordinatorClient;

  delete memoryAllocator;

  StatusPrinter::add("Phase 3 complete");
  StatusPrinter::flush();
}

/**
   Derive additional parameters from those provided by the user
*/
void deriveAdditionalParams(
  Params& params, StringList& intermediateDiskList,
  StringList& outputDiskList) {
  // Compute the number of disks per writer.
  uint64_t numIntermediateDisks = intermediateDiskList.size();
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_zero", numIntermediateDisks);
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_one", numIntermediateDisks);

  uint64_t numPhaseOneWriters =
    params.get<uint64_t>("NUM_WORKERS.phase_one.writer");
  ABORT_IF(numIntermediateDisks % numPhaseOneWriters != 0,
           "Number of phase one writers (%llu) must divide the number of "
           "intermediate disks (%llu)", numPhaseOneWriters,
           numIntermediateDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_one.writer",
    numIntermediateDisks / numPhaseOneWriters);

  uint64_t numOutputDisks = outputDiskList.size();
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_two", numOutputDisks);
  uint64_t numPhaseTwoWriters =
    params.get<uint64_t>("NUM_WORKERS.phase_two.writer");
  ABORT_IF(numOutputDisks % numPhaseTwoWriters != 0,
           "Number of phase two writers (%llu) must divide the number of "
           "output disks (%llu)", numPhaseTwoWriters, numOutputDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_two.writer", numOutputDisks / numPhaseTwoWriters);

  // Phase three writes to intermediate disks for splitsort and output disks for
  // mergereduce
  uint64_t numSplitSortWriters =
    params.get<uint64_t>("NUM_WORKERS.phase_three.splitsort_writer");
  ABORT_IF(numIntermediateDisks % numSplitSortWriters != 0,
           "Number of phase three split sort writers (%llu) must divide the "
           "number of intermediate disks (%llu)",
           numSplitSortWriters, numIntermediateDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_three.splitsort_writer",
    numIntermediateDisks / numSplitSortWriters);

  uint64_t numMergeReduceWriters =
    params.get<uint64_t>("NUM_WORKERS.phase_three.mergereduce_writer");
  ABORT_IF(numOutputDisks % numMergeReduceWriters != 0,
           "Number of phase three merge reduce writers (%llu) must divide the "
           "number of output disks (%llu)",
           numMergeReduceWriters, numOutputDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_three.mergereduce_writer",
    numOutputDisks / numMergeReduceWriters);



  // Compute the number of disks per chainer.
  uint64_t numChainers = params.get<uint64_t>("NUM_WORKERS.phase_one.chainer");
  ABORT_IF(numIntermediateDisks % numChainers != 0,
           "Number of chainers (%llu) must divide the number of "
           "intermediate disks (%llu)", numChainers, numIntermediateDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_one.chainer", numIntermediateDisks / numChainers);

  // Compute the number of disks per coalescer.
  uint64_t numCoalescers =
    params.get<uint64_t>("NUM_WORKERS.phase_one.coalescer");
  ABORT_IF(numIntermediateDisks % numCoalescers != 0,
           "Number of coalescers (%llu) must divide the number of "
           "intermediate disks (%llu)", numCoalescers, numIntermediateDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_one.coalescer",
    numIntermediateDisks / numCoalescers);

  // Use one partition group per demux.
  uint64_t numDemuxes = params.get<uint64_t>("NUM_WORKERS.phase_one.demux");
  params.add<uint64_t>("PARTITION_GROUPS_PER_NODE", numDemuxes);
  params.add<uint64_t>(
    "NUM_PARTITION_GROUPS", numDemuxes * params.get<uint64_t>("NUM_PEERS"));

  // Set phase 0 and 1 format reader type
  params.add<std::string>(
    "FORMAT_READER.phase_zero",
    params.get<std::string>("MAP_INPUT_FORMAT_READER"));
  params.add<std::string>(
    "FORMAT_READER.phase_one",
    params.get<std::string>("MAP_INPUT_FORMAT_READER"));

  // Possibly set phase 2 and 3 format reader type
  if (params.contains("REDUCE_INPUT_FORMAT_READER")) {
    // The user wants to use a byte stream converter in phase 2 to change the
    // reduce input data format.
    params.add<std::string>(
      "FORMAT_READER.phase_two",
      params.get<std::string>("REDUCE_INPUT_FORMAT_READER"));
    params.add<std::string>(
      "FORMAT_READER.phase_three",
      params.get<std::string>("REDUCE_INPUT_FORMAT_READER"));
  }

  // Sets pipeline-specific serialization/deserialization parameters.
  if (params.get<bool>("WRITE_WITHOUT_HEADERS.phase_one")) {
    // Mapper will serialize without headers to strip them from network I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.mapper", true);
    // Demux will deserialize mapper buffers without headers.
    params.add<bool>("DESERIALIZE_WITHOUT_HEADERS.phase_one.demux", true);
    // Demux will serialize buffers without headers to strip them from disk I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.demux", true);
  }

  if (params.get<bool>("WRITE_WITHOUT_HEADERS.phase_two")) {
    // Reducer will serialize without headers to strip them from disk I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_two.reducer", true);
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_three.reducer", true);
  }

  if (!params.contains("FORMAT_READER.phase_two")) {
    // We won't be using the converter or its quota.
    params.add<uint64_t>("NUM_WORKERS.phase_two.reader_converter", 0);
    params.add<uint64_t>("MEMORY_QUOTAS.phase_two.reader_converter", 0);
  }

  // Use only 90% of the available memory for the allocator
  params.add<uint64_t>(
    "ALLOCATOR_CAPACITY", floor(0.9 * params.get<uint64_t>("MEM_SIZE")));

  // Set alignment parameters:
  uint64_t alignmentMultiple = params.get<uint64_t>("ALIGNMENT_MULTIPLE");
  if (params.get<bool>("DIRECT_IO.phase_zero.reader")) {
    // Phase zero reader should align buffers.
    params.add<uint64_t>("ALIGNMENT.phase_zero.reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_one.reader")) {
    // Phase one reader should align buffers.
    params.add<uint64_t>("ALIGNMENT.phase_one.reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_one.writer")) {
    if (params.get<bool>("USE_WRITE_CHAINING")) {
      // Phase one coalescer should align buffers since we're chaining.
      params.add<uint64_t>("ALIGNMENT.phase_one.coalescer", alignmentMultiple);
    } else {
      // Phase one demux should align buffers since we're not chaining, and a
      // writer stage immediately follows the demux.
      params.add<uint64_t>("ALIGNMENT.phase_one.demux", alignmentMultiple);
    }

    // Writer needs to know that it should write with alignment.
    params.add<uint64_t>("ALIGNMENT.phase_one.writer", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_two.reader")) {
    // Phase two reader should align buffers.
    params.add<uint64_t>("ALIGNMENT.phase_two.reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_two.writer")) {
    // Phase two reducers should align buffers since it immediately precedes the
    // writer stage.
    params.add<uint64_t>("ALIGNMENT.phase_two.reducer", alignmentMultiple);

    // Writer needs to know that it should write with alignment.
    params.add<uint64_t>("ALIGNMENT.phase_two.writer", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_three.splitsort_reader")) {
    // Phase three splitsort reader should align buffers.
    params.add<uint64_t>(
      "ALIGNMENT.phase_three.splitsort_reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_three.mergereduce_reader")) {
    // Phase three mergereduce reader should align buffers.
    params.add<uint64_t>(
      "ALIGNMENT.phase_three.mergereduce_reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_three.splitsort_writer")) {
    params.add<uint64_t>("ALIGNMENT.phase_three.sorter", alignmentMultiple);
    params.add<uint64_t>(
      "ALIGNMENT.phase_three.splitsort_writer", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_three.mergereduce_writer")) {
    params.add<uint64_t>("ALIGNMENT.phase_three.reducer", alignmentMultiple);
    params.add<uint64_t>(
      "ALIGNMENT.phase_three.mergereduce_writer", alignmentMultiple);
  }

  // Stages that use fixed-size buffers should use caching allocators.
  // Phase 0 = reader
  if (params.contains("CACHED_MEMORY.phase_zero.reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_zero.reader", true);
  }

  // Phase one = reader and demux
  if (params.contains("CACHED_MEMORY.phase_one.reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_one.reader", true);
  }

  if (params.contains("CACHED_MEMORY.phase_one.demux")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_one.demux", true);
  }

  // If we're using a format reader in phase two then we can use a caching
  // allocator.
  if (params.contains("FORMAT_READER.phase_two") &&
      params.contains("CACHED_MEMORY.phase_two.reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_two.reader", true);
  }

  if (params.contains("CACHED_MEMORY.phase_three.splitsort_reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_three.splitsort_reader", true);
  }

  if (params.contains("CACHED_MEMORY.phase_three.mergereduce_reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_three.mergereduce_reader", true);
  }

  // Phase zero senders and phase two replica sender require data to be received
  // in-order, so force a single TCP connection per peer.
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_zero.coordinator_sender", 1);
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_zero.broadcast_sender", 1);
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_two.replica_sender", 1);
}

int main(int argc, char** argv) {
  HDFSClient::init();

  StatLogger* mainLogger = new StatLogger("mapreduce");

  Timer totalTimer;
  totalTimer.start();

  uint64_t randomSeed = Timer::posixTimeInMicros() * getpid();
  srand(randomSeed);
  srand48(randomSeed);

  signal(SIGBUS, TritonSortAssertions::dumpStack);
  // Gather StackTrace for segfault
  signal(SIGSEGV, sigsegvHandler);

  Params params;
  params.parseCommandLine(argc, argv);

  StatusPrinter::init(&params);
  StatusPrinter::spawn();

  StatWriter::init(params);
  StatWriter::spawn();

  ResourceMonitor::init(&params);
  ResourceMonitor::spawn();

  IntervalStatLogger::init(&params);
  IntervalStatLogger::spawn();

  // Connect to Redis.
  const std::string& coordinatorClient =
    params.get<std::string>("COORDINATOR_CLIENT");
  if (coordinatorClient == "redis") {
    RedisConnection::connect(params);
  }

  logStartTime();

  // Parse disk lists from params.
  StringList intermediateDiskList;
  getDiskList(intermediateDiskList, "INTERMEDIATE_DISK_LIST", &params);
  mainLogger->logDatum("num_intermediate_disks", intermediateDiskList.size());

  StringList outputDiskList;
  getDiskList(outputDiskList, "OUTPUT_DISK_LIST", &params);

  // Parse peer list.
  IPList peers;
  parsePeerList(params, peers);

  // Calculate additional params based on the existing parameter set
  deriveAdditionalParams(params, intermediateDiskList, outputDiskList);

  RecordFilterMap recordFilterMap(params);

  KeyPartitionerInterface* keyPartitioner = NULL;

  if (!params.get<bool>("SKIP_PHASE_ZERO")) {
    PhaseZeroOutputData* outputData = executePhaseZero(
      &params, peers, intermediateDiskList, recordFilterMap);
    keyPartitioner = outputData->getKeyPartitioner();

    delete outputData;
  }

  if (keyPartitioner != NULL) {
    File keyPartitionerFile(params.get<std::string>("BOUNDARY_LIST_FILE"));
    keyPartitioner->writeToFile(keyPartitionerFile);
  }

  if (!params.get<bool>("SKIP_PHASE_ONE")) {
    // In the event that we skipped phase zero for a job we're going to need to
    // fill in the partition count.
    uint64_t localNodeID = params.get<uint64_t>("MYPEERID");
    CoordinatorClientInterface* coordinatorClient =
      CoordinatorClientFactory::newCoordinatorClient(params, "", "", 0);

    std::list<uint64_t> jobIDList;
    parseCommaDelimitedList< uint64_t, std::list<uint64_t> >(
      jobIDList, params.get<std::string>("JOB_IDS"));
    for (std::list<uint64_t>::iterator iter = jobIDList.begin();
         iter != jobIDList.end(); iter++) {
      JobInfo* jobInfo = coordinatorClient->getJobInfo(*iter);
      if (jobInfo->numPartitions == 0) {
        // Partition count not set for this job.
        if (localNodeID == 0) {
          // We are responsible for setting the partition count.
          // Use user-defined Intermediate:Input ratio.
          setNumPartitions(
            jobInfo->jobID, params.get<double>("INTERMEDIATE_TO_INPUT_RATIO"),
            params);
        } else {
          // Peer 0 should set this shortly, check every 100ms
          do {
            delete jobInfo;
            usleep(100000);
            jobInfo = coordinatorClient->getJobInfo(*iter);
          } while (jobInfo->numPartitions == 0);
        }
      }
      delete jobInfo;
    }

    delete coordinatorClient;

    // Phase one uses SimpleMemoryAllocator, may try to allocate more memory
    // than is available, so limit the memory usage to prevent the OOM killer
    // from killing us.
    limitMemorySize(params);

    executePhaseOne(&params, peers, intermediateDiskList, recordFilterMap);
  }

  if (keyPartitioner != NULL) {
    delete keyPartitioner;
  }

  if (!params.get<bool>("SKIP_PHASE_TWO")) {
    // Phase two also uses SimpleMemoryAllocator, so limit the memory usage to
    // prevent the OOM killer from killing us.
    limitMemorySize(params);

    executePhaseTwo(&params, peers, intermediateDiskList, outputDiskList);
  }

  if (!params.get<bool>("SKIP_PHASE_THREE")) {
    limitMemorySize(params);

    executePhaseThree(&params, peers, intermediateDiskList, outputDiskList);
  }

  dumpParams(params);

  totalTimer.stop();

  ResourceMonitor::teardown();

  IntervalStatLogger::teardown();

  mainLogger->logDatum("total_runtime", totalTimer);

  // Delete logger so that it deregisters
  delete mainLogger;

  StatWriter::teardown();

  StatusPrinter::teardown();

  // Disconnect from redis.
  if (coordinatorClient == "redis") {
    RedisConnection::disconnect();
  }

  HDFSClient::cleanup();

  return 0;
}
