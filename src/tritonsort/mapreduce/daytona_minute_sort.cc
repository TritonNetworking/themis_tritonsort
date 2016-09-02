#include <cmath>
#include <signal.h>
#include <stdlib.h>

#include "tritonsort/config.h"

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30"
#endif // USE_JEMALLOC

#include "../common/MainUtils.h"
#include "common/BufferListContainerFactory.h"
#include "common/ByteStreamBufferFactory.h"
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
#include "mapreduce/common/queueing/MapReduceWorkQueueingPolicyFactory.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"
#include "mapreduce/functions/reduce/ReduceFunctionFactory.h"
#include "mapreduce/workers/MapReduceWorkerImpls.h"

typedef BufferListContainer<ListableKVPairBuffer> KVPBContainer;

/// Copypasta from main.cc
/// TODO: Unify this
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

  // Open TCP sockets.
  SocketArray shuffleSenderSockets;
  SocketArray shuffleReceiverSockets;
  openAllSockets(
    params->get<std::string>("SHUFFLE_PORT"), peers.size(), *params,
    phaseName, "shuffle_sender", peers, peers, shuffleSenderSockets,
    shuffleReceiverSockets);

  SocketArray scannerSenderSockets;
  SocketArray scannerReceiverSockets;
  openAllSockets(
    params->get<std::string>("SCANNER_PORT"), peers.size(), *params,
    phaseName, "scanner_sender", peers, peers, scannerSenderSockets,
    scannerReceiverSockets);

  SocketArray deciderSenderSockets;
  SocketArray deciderReceiverSockets;
  openAllSockets(
    params->get<std::string>("DECIDER_PORT"), peers.size(), *params,
    phaseName, "decider_sender", peers, peers, deciderSenderSockets,
    deciderReceiverSockets);

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
  QuotaEnforcingWorkerTracker scannerSenderTracker(
    *params, phaseName, "scanner_sender", &queueingPolicyFactory);


  WorkerTracker scannerReceiverTracker(
    *params, phaseName, "scanner_receiver", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker boundaryDeciderTracker(
    *params, phaseName, "boundary_decider", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker deciderSenderTracker(
    *params, phaseName, "decider_sender", &queueingPolicyFactory);

  WorkerTracker deciderReceiverTracker(
    *params, phaseName, "decider_receiver", &queueingPolicyFactory);
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
    "scanner_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.boundary_scanner"));
  MemoryQuota scannerReceiverQuota(
    "scanner_receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_zero.scanner_receiver"));
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
  scannerSenderTracker.addProducerQuota(boundaryScannerQuota);
  scannerSenderTracker.addConsumerQuota(boundaryScannerQuota);
  boundaryDeciderTracker.addProducerQuota(scannerReceiverQuota);
  boundaryDeciderTracker.addConsumerQuota(scannerReceiverQuota);
  deciderSenderTracker.addProducerQuota(boundaryDeciderQuota);
  deciderSenderTracker.addConsumerQuota(boundaryDeciderQuota);

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
  phaseZeroTrackers.addTracker(&scannerSenderTracker);
  phaseZeroTrackers.addTracker(&deciderSenderTracker);
  phaseZeroTrackers.addTracker(&deciderReceiverTracker, true);
  phaseZeroTrackers.addTracker(&scannerReceiverTracker, true);
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
  boundaryScannerTracker.addDownstreamTracker(&scannerSenderTracker);
  scannerReceiverTracker.isSourceTracker();
  scannerReceiverTracker.addDownstreamTracker(&boundaryDeciderTracker);
  deciderReceiverTracker.isSourceTracker();
  deciderReceiverTracker.addDownstreamTracker(&boundaryDeserializerTracker);
  boundaryDeciderTracker.addDownstreamTracker(&deciderSenderTracker);
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
  scannerSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");

  scannerReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  deciderReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");

  boundaryDeciderTracker.setFactory(
    &workerFactory, "mapreduce", "boundary_decider");

  boundaryDeserializerTracker.setFactory(
    &workerFactory, "mapreduce", "boundary_deserializer");

  deciderSenderTracker.setFactory(
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
    "scanner_sender", "sockets", &scannerSenderSockets);
  workerFactory.addDependency(
    "scanner_receiver", "sockets", &scannerReceiverSockets);

  workerFactory.addDependency(
    "decider_sender", "sockets", &deciderSenderSockets);
  workerFactory.addDependency(
    "decider_receiver", "sockets", &deciderReceiverSockets);

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
  Params* params, const StringList& intermediateDiskList,
  IPList& peers, RecordFilterMap& recordFilterMap) {

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

  uint64_t localNodeID = params->get<uint64_t>("MYPEERID");
  uint64_t numNodes = params->get<uint64_t>("NUM_PEERS");

  IPList replicaSenderPeers;
  for (uint64_t i = 1; i < numNodes; i++) {
    // Replicate to consecutive nodes.
    replicaSenderPeers.push_back(peers.at((localNodeID + i) % numNodes));
  }

  // Open TCP connections to replica nodes.
  uint64_t replicationLevel = params->get<uint64_t>("OUTPUT_REPLICATION_LEVEL");
  SocketArray replicaSenderSockets;
  SocketArray replicaReceiverSockets;
  if (replicationLevel > 1) {
    openAllSockets(
      params->get<std::string>("REPLICA_RECEIVER_PORT"), numNodes - 1,
      *params, phaseName, "replica_sender", replicaSenderPeers, peers,
      replicaSenderSockets, replicaReceiverSockets);
  }

  // Block until all nodes have connected TCP sockets.
  coordinatorClient->waitOnBarrier("sockets_connected");
  delete coordinatorClient;

  // Set up trackers and plumb workers together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;

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
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reader"));
  MemoryQuota readerConverterQuota(
    "reader_converter_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reader_converter"));
  MemoryQuota mapperQuota(
    "mapper_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.mapper"));
  MemoryQuota receiverQuota(
    "receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.receiver"));
  MemoryQuota demuxQuota(
    "demux_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.demux"));
  MemoryQuota sorterQuota(
    "sorter_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.sorter"));
  MemoryQuota reducerQuota(
    "reducer_quota", params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reducer"));

  MemoryQuota reducerReplicaQuota(
    "reducer_replica_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.phase_one.reducer_replica"));

  readerConverterTracker.addProducerQuota(readerQuota);
  readerConverterTracker.addConsumerQuota(readerQuota);
  mapperTracker.addProducerQuota(readerConverterQuota);
  mapperTracker.addConsumerQuota(readerConverterQuota);
  senderTracker.addProducerQuota(mapperQuota);
  senderTracker.addConsumerQuota(mapperQuota);
  tupleDemuxTracker.addProducerQuota(receiverQuota);
  tupleDemuxTracker.addConsumerQuota(receiverQuota);
  sorterTracker.addProducerQuota(demuxQuota);
  sorterTracker.addConsumerQuota(demuxQuota);
  reducerTracker.addProducerQuota(sorterQuota);
  reducerTracker.addConsumerQuota(sorterQuota);
  writerTracker.addProducerQuota(reducerQuota);
  writerTracker.addConsumerQuota(reducerQuota);

  if (replicationLevel > 1) {
    replicaSenderTracker.addProducerQuota(reducerReplicaQuota);
    replicaSenderTracker.addConsumerQuota(reducerReplicaQuota);
  }

  TrackerSet phaseOneTrackers;
  phaseOneTrackers.addTracker(&readerTracker, true);
  phaseOneTrackers.addTracker(&readerConverterTracker);
  phaseOneTrackers.addTracker(&mapperTracker);
  phaseOneTrackers.addTracker(&senderTracker);
  phaseOneTrackers.addTracker(&receiverTracker, true);
  phaseOneTrackers.addTracker(&tupleDemuxTracker);
  phaseOneTrackers.addTracker(&sorterTracker);
  phaseOneTrackers.addTracker(&reducerTracker);
  phaseOneTrackers.addTracker(&writerTracker);

  if (replicationLevel > 1) {
    phaseOneTrackers.addTracker(&replicaSenderTracker);
    phaseOneTrackers.addTracker(&replicaReceiverTracker, true);
  }

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&readerConverterTracker);
  readerConverterTracker.addDownstreamTracker(&mapperTracker);
  mapperTracker.addDownstreamTracker(&senderTracker);

  receiverTracker.isSourceTracker();
  receiverTracker.addDownstreamTracker(&tupleDemuxTracker);
  tupleDemuxTracker.addDownstreamTracker(&sorterTracker);
  sorterTracker.addDownstreamTracker(&reducerTracker);
  reducerTracker.addDownstreamTracker(&writerTracker);

  // Escape hatch for large partitions.
  tupleDemuxTracker.addDownstreamTracker(&writerTracker);

  if (replicationLevel > 1) {
    reducerTracker.addDownstreamTracker(&replicaSenderTracker);
    replicaReceiverTracker.isSourceTracker();
    replicaReceiverTracker.addDownstreamTracker(&writerTracker);
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
  sorterTracker.setFactory(&workerFactory, "mapreduce", "sorter");
  reducerTracker.setFactory(&workerFactory, "mapreduce", "reducer");
  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  replicaSenderTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_sender");
  replicaReceiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");


  // Set up resources
  ABORT_IF(intermediateDiskList.size() == 0, "Must specify more than one "
           "intermediate directory");

  // Inject dependencies into factories

  std::string converterStageName("reader_converter");

  workerFactory.addDependency("sender", "sockets", &senderSockets);
  workerFactory.addDependency("receiver", "sockets", &receiverSockets);

  workerFactory.addDependency(
    "reader", "filename_to_stream_id_map", &inputFilenameToStreamIDMap);
  workerFactory.addDependency(
    "reader", "converter_stage_name", &converterStageName);
  workerFactory.addDependency(
    "reader_converter", "filename_to_stream_id_map",
    &inputFilenameToStreamIDMap);

  PartitionFunctionMap partitionFunctionMap(*params, phaseName);
  workerFactory.addDependency("partition_function_map", &partitionFunctionMap);
  workerFactory.addDependency("record_filter_map", &recordFilterMap);

  workerFactory.addDependency(
    "output_disk_list", const_cast<StringList*>(&intermediateDiskList));

  workerFactory.addDependency(
    "replica_sender", "sockets", &replicaSenderSockets);
  workerFactory.addDependency(
    "replica_receiver", "sockets", &replicaReceiverSockets);

  bool minutesort = true;
  workerFactory.addDependency("minutesort", &minutesort);

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

/// Copypasta from main.cc
/// TODO: Unify this
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
    const themis::URL& outputDirectory =
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
    const themis::URL& outputDirectory =
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
  uint64_t numOutputDisks = outputDiskList.size();
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_zero", numIntermediateDisks);
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_one", numIntermediateDisks);
  params.add<uint64_t>("NUM_OUTPUT_DISKS.phase_two", numOutputDisks);

  uint64_t numPhaseOneWriters =
    params.get<uint64_t>("NUM_WORKERS.phase_one.writer");
  ABORT_IF(numIntermediateDisks % numPhaseOneWriters != 0,
           "Number of phase one writers (%llu) must divide the number of "
           "intermediate disks (%llu)", numPhaseOneWriters,
           numIntermediateDisks);
  params.add<uint64_t>(
    "DISKS_PER_WORKER.phase_one.writer",
    numIntermediateDisks / numPhaseOneWriters);


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


  // Use one partition group per demux.
  uint64_t numDemuxes = params.get<uint64_t>("NUM_WORKERS.phase_one.demux");
  params.add<uint64_t>("PARTITION_GROUPS_PER_NODE", numDemuxes);
  params.add<uint64_t>(
    "NUM_PARTITION_GROUPS", numDemuxes * params.get<uint64_t>("NUM_PEERS"));

  params.add<std::string>(
    "FORMAT_READER.phase_zero",
    params.get<std::string>("MAP_INPUT_FORMAT_READER"));
  params.add<std::string>(
    "FORMAT_READER.phase_one",
    params.get<std::string>("MAP_INPUT_FORMAT_READER"));

  // Demux output will have headers, so require it in phase three
  params.add<std::string>(
    "FORMAT_READER.phase_three", "KVPairFormatReader");

  // Sets pipeline-specific serialization/deserialization parameters.
  if (params.get<bool>("WRITE_WITHOUT_HEADERS.phase_one")) {
    // Mapper will serialize without headers to strip them from network I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.mapper", true);
    // Demux will deserialize mapper buffers without headers.
    params.add<bool>("DESERIALIZE_WITHOUT_HEADERS.phase_one.demux", true);
    // Reducer will serialize without headers to strip them from disk I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.reducer", true);
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_three.reducer", true);
  }

  // Use only 90% of the available memory for the allocator
  params.add<uint64_t>(
      "ALLOCATOR_CAPACITY", std::floor(0.9 * params.get<uint64_t>("MEM_SIZE")));

  // Set alignment parameters:
  uint64_t alignmentMultiple = params.get<uint64_t>("ALIGNMENT_MULTIPLE");
  if (params.get<bool>("DIRECT_IO.phase_one.reader")) {
    // Phase one reader should align buffers.
    params.add<uint64_t>("ALIGNMENT.phase_one.reader", alignmentMultiple);
  }

  if (params.get<bool>("DIRECT_IO.phase_one.writer")) {
    // Reducer comes before the writer.
    params.add<uint64_t>("ALIGNMENT.phase_one.reducer", alignmentMultiple);
    // Replica receiver needs to be aligned as well, since it feeds into writer.
    params.add<uint64_t>("ALIGNMENT.phase_one.replica_receiver",
                         alignmentMultiple);
    // Demux ALSO feeds into writer.
    params.add<uint64_t>("ALIGNMENT.phase_one.demux", alignmentMultiple);
    // Writer needs to know that it should write with alignment.
    params.add<uint64_t>("ALIGNMENT.phase_one.writer", alignmentMultiple);
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
    params.add<uint64_t>("ALIGNMENT.phase_three.replica_receiver",
                         alignmentMultiple);
    params.add<uint64_t>(
      "ALIGNMENT.phase_three.mergereduce_writer", alignmentMultiple);
  }


  // Stages that use fixed-size buffers should use caching allocators.
  // Phase one = reader and demux
  if (params.contains("CACHED_MEMORY.phase_one.reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_one.reader", true);
  }

  // Phase zero senders and phase two replica sender require data to be received
  // in-order, so force a single TCP connection per peer.
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_zero.scanner_sender", 1);
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_zero.decider_sender", 1);
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_one.replica_sender", 1);
  params.add<uint64_t>("SOCKETS_PER_PEER.phase_three.replica_sender", 1);
}

int main(int argc, char** argv) {
  StatLogger* mainLogger = new StatLogger("mapreduce");

  Timer totalTimer;
  totalTimer.start();

  uint64_t randomSeed = Timer::posixTimeInMicros() * getpid();
  srand(randomSeed);
  srand48(randomSeed);

  signal(SIGSEGV, sigsegvHandler);

  Params params;
  params.parseCommandLine(argc, argv);

  ABORT_IF(
    params.get<bool>("USE_WRITE_CHAINING"),
    "MinuteSort does not support write chaining");

  StatusPrinter::init(&params);
  StatusPrinter::spawn();

  StatWriter::init(params);
  StatWriter::spawn();

  ResourceMonitor::init(&params);
  ResourceMonitor::spawn();

  IntervalStatLogger::init(&params);
  IntervalStatLogger::spawn();

  // Connect to Redis.
  const std::string& coordinatorClientType =
    params.get<std::string>("COORDINATOR_CLIENT");
  if (coordinatorClientType == "redis") {
    RedisConnection::connect(params);
  }

  logStartTime();

  // Parse peer list.
  IPList peers;
  parsePeerList(params, peers);

  // Parse disk lists from params.
  StringList intermediateDiskList;
  getDiskList(intermediateDiskList, "INTERMEDIATE_DISK_LIST", &params);
  mainLogger->logDatum("num_intermediate_disks", intermediateDiskList.size());

  StringList outputDiskList;
  // For ioSort, we want the output disks to be the same as the intermediates.
  getDiskList(outputDiskList, "INTERMEDIATE_DISK_LIST", &params);

  // Calculate additional params based on the existing parameter set
  deriveAdditionalParams(params, intermediateDiskList, outputDiskList);

  RecordFilterMap recordFilterMap(params);

  limitMemorySize(params);

  KeyPartitionerInterface* keyPartitioner = NULL;

  // Run phase zero.
  PhaseZeroOutputData* outputData = executePhaseZero(
    &params, peers, intermediateDiskList, recordFilterMap);
  keyPartitioner = outputData->getKeyPartitioner();

  if (keyPartitioner != NULL) {
    File keyPartitionerFile(params.get<std::string>("BOUNDARY_LIST_FILE"));
    keyPartitioner->writeToFile(keyPartitionerFile);
  }

  // Node 0 should set the number of partitions
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

  // Run phase one
  executePhaseOne(
    &params, intermediateDiskList, peers, recordFilterMap);

  // Run phase three
  executePhaseThree(&params, peers, intermediateDiskList, outputDiskList);

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
  if (coordinatorClientType == "redis") {
    RedisConnection::disconnect();
  }

  return 0;
}
