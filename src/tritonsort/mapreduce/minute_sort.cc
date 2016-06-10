#include <cmath>
#include <signal.h>
#include <stdlib.h>

#include "tritonsort/config.h"

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30";
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

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&readerConverterTracker);
  readerConverterTracker.addDownstreamTracker(&mapperTracker);
  mapperTracker.addDownstreamTracker(&senderTracker);

  receiverTracker.isSourceTracker();
  receiverTracker.addDownstreamTracker(&tupleDemuxTracker);
  tupleDemuxTracker.addDownstreamTracker(&sorterTracker);
  sorterTracker.addDownstreamTracker(&reducerTracker);
  reducerTracker.addDownstreamTracker(&writerTracker);

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

/**
   Derive additional parameters from those provided by the user
*/
void deriveAdditionalParams(
  Params& params, StringList& intermediateDiskList,
  StringList& outputDiskList) {
  // Compute the number of disks per writer.
  uint64_t numIntermediateDisks = intermediateDiskList.size();
  uint64_t numOutputDisks = outputDiskList.size();
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

  // Use one partition group per demux.
  uint64_t numDemuxes = params.get<uint64_t>("NUM_WORKERS.phase_one.demux");
  params.add<uint64_t>("PARTITION_GROUPS_PER_NODE", numDemuxes);
  params.add<uint64_t>(
    "NUM_PARTITION_GROUPS", numDemuxes * params.get<uint64_t>("NUM_PEERS"));

  params.add<std::string>(
    "FORMAT_READER.phase_one",
    params.get<std::string>("MAP_INPUT_FORMAT_READER"));

  // Sets pipeline-specific serialization/deserialization parameters.
  if (params.get<bool>("WRITE_WITHOUT_HEADERS.phase_one")) {
    // Mapper will serialize without headers to strip them from network I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.mapper", true);
    // Demux will deserialize mapper buffers without headers.
    params.add<bool>("DESERIALIZE_WITHOUT_HEADERS.phase_one.demux", true);
    // Reducer will serialize without headers to strip them from disk I/O.
    params.add<bool>("SERIALIZE_WITHOUT_HEADERS.phase_one.reducer", true);
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
    // Writer needs to know that it should write with alignment.
    params.add<uint64_t>("ALIGNMENT.phase_one.writer", alignmentMultiple);
  }

  // Stages that use fixed-size buffers should use caching allocators.
  // Phase one = reader and demux
  if (params.contains("CACHED_MEMORY.phase_one.reader")) {
    params.add<bool>("CACHING_ALLOCATOR.phase_one.reader", true);
  }
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
  getDiskList(outputDiskList, "OUTPUT_DISK_LIST", &params);

  // Calculate additional params based on the existing parameter set
  deriveAdditionalParams(params, intermediateDiskList, outputDiskList);

  RecordFilterMap recordFilterMap(params);

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

  // Phase one uses SimpleMemoryAllocator, may try to allocate more memory
  // than is available, so limit the memory usage to prevent the OOM killer
  // from killing us.
  limitMemorySize(params);

  executePhaseOne(
    &params, intermediateDiskList, peers, recordFilterMap);

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
