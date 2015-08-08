#include <math.h>
#include <signal.h>
#include <stdlib.h>

#include "config.h"

#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30";

#include "benchmarks/mixediobench/workers/MixedIOBenchWorkerImpls.h"
#include "benchmarks/storagebench/workers/StorageBenchWorkerImpls.h"
#include "common/MainUtils.h"
#include "common/SimpleMemoryAllocator.h"
#include "core/CPUAffinitySetter.h"
#include "core/IntervalStatLogger.h"
#include "core/MemoryQuota.h"
#include "core/Params.h"
#include "core/QuotaEnforcingWorkerTracker.h"
#include "core/ResourceMonitor.h"
#include "core/StatWriter.h"
#include "core/ThreadSafeVector.h"
#include "core/Timer.h"
#include "core/TrackerSet.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/WorkerTracker.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/Utils.h"
#include "mapreduce/common/queueing/MapReduceWorkQueueingPolicyFactory.h"
#include "mapreduce/workers/MapReduceWorkerImpls.h"

void executeBenchmark(Params* params) {
  std::string phaseName("mixediobench");

  StatLogger phaseStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  // Parse peer list.
  IPList peers;
  parsePeerList(*params, peers);

  // Open all-to-all TCP sockets.
  SocketArray senderSockets;
  SocketArray receiverSockets;
  openAllSockets(
    params->get<std::string>("RECEIVER_PORT"), peers.size(), *params, phaseName,
    "sender", peers, peers, senderSockets, receiverSockets);

  // Adjust partition size so we get roughly the correct amount of data.
  uint64_t dataSize = params->getv<uint64_t>(
    "BENCHMARK_DATA_SIZE_PER_NODE.%s", phaseName.c_str());
  uint64_t partitionSize = params->get<uint64_t>("PARTITION_SIZE");
  uint64_t numPartitionsPerNode = dataSize / partitionSize;
  // New partition size might be slightly larger than what the user wanted but
  // will more evenly divide the data size.
  params->add<uint64_t>("PARTITION_SIZE", dataSize / numPartitionsPerNode);

  // Set the partition groups
  uint64_t numPeers = params->get<uint64_t>("NUM_PEERS");
  params->add<uint64_t>("NUM_PARTITIONS", numPartitionsPerNode * numPeers);
  params->add("PARTITION_GROUPS_PER_NODE", numPartitionsPerNode);
  params->add("NUM_PARTITION_GROUPS", numPartitionsPerNode * numPeers);

  // Set alignment parameters:
  uint64_t alignmentMultiple = params->get<uint64_t>("ALIGNMENT_MULTIPLE");
  if (params->getv<bool>("DIRECT_IO.%s.reader", phaseName.c_str())) {
    // Reader should align buffers.
    params->add<uint64_t>(
      "ALIGNMENT." + phaseName + ".reader", alignmentMultiple);
  }

  if (params->getv<bool>("DIRECT_IO.%s.writer", phaseName.c_str())) {
    // Receiver should align buffers for the writer.
    params->add<uint64_t>(
      "ALIGNMENT." + phaseName + ".receiver", alignmentMultiple);

    // Writer needs to know that it should write with alignment.
    params->add<uint64_t>(
      "ALIGNMENT." + phaseName + ".writer", alignmentMultiple);
  }

  // The reader can use a caching allocator since it allocates fixed size
  // buffers.
  if (params->contains("CACHED_MEMORY.mixediobench.reader")) {
    params->add<bool>("CACHING_ALLOCATOR.mixediobench.reader", true);
  }

  // Load disk lists
  StringList inputDiskList;
  std::string inputDiskListParam = "INPUT_DISK_LIST." + phaseName;
  getDiskList(inputDiskList, inputDiskListParam, params);
  params->add<uint64_t>("NUM_INPUT_DISKS", inputDiskList.size());
  StringList outputDiskList;
  std::string outputDiskListParam = "OUTPUT_DISK_LIST." + phaseName;
  getDiskList(outputDiskList, outputDiskListParam, params);

  // Compute disks per reader/writer
  uint64_t numReaders = params->getv<uint64_t>(
    "NUM_WORKERS.%s.reader", phaseName.c_str());
  params->add<uint64_t>(
    "DISKS_PER_WORKER." + phaseName + ".reader",
    inputDiskList.size() / numReaders);
  uint64_t numWriters = params->getv<uint64_t>(
    "NUM_WORKERS.%s.writer", phaseName.c_str());
  params->add<uint64_t>(
      "DISKS_PER_WORKER." + phaseName + ".writer",
      outputDiskList.size() / numWriters);
  params->add<uint64_t>(
    "NUM_OUTPUT_DISKS." + phaseName, outputDiskList.size());

  // Set up trackers and plumb workers together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;
  WorkerTracker readerTracker(
    *params, phaseName, "reader", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker taggerTracker(
    *params, phaseName, "tagger", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker senderTracker(
    *params, phaseName, "sender", &queueingPolicyFactory);

  WorkerTracker receiverTracker(
    *params, phaseName, "receiver", &queueingPolicyFactory);
  // Not actually a really tuple demux. This demux simply sets the partition ID
  // to be the partition group.
  QuotaEnforcingWorkerTracker demuxTracker(
    *params, phaseName, "demux", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker writerTracker(
    *params, phaseName, "writer", &queueingPolicyFactory);

  // Quotas between reader -> sender, receiver -> writer
  MemoryQuota readerQuota(
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.mixediobench.reader"));
  MemoryQuota receiverQuota(
    "receiver_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.mixediobench.receiver"));
  taggerTracker.addProducerQuota(readerQuota);
  senderTracker.addConsumerQuota(readerQuota);
  demuxTracker.addProducerQuota(receiverQuota);
  writerTracker.addConsumerQuota(receiverQuota);

  TrackerSet trackers;
  trackers.addTracker(&readerTracker, true);
  trackers.addTracker(&taggerTracker);
  trackers.addTracker(&senderTracker);

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&taggerTracker);
  taggerTracker.addDownstreamTracker(&senderTracker);

  trackers.addTracker(&receiverTracker, true);
  trackers.addTracker(&demuxTracker);
  trackers.addTracker(&writerTracker);

  receiverTracker.isSourceTracker();
  receiverTracker.addDownstreamTracker(&demuxTracker);
  demuxTracker.addDownstreamTracker(&writerTracker);

  loadReadRequests(readerTracker, *params, phaseName);

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;
  StorageBenchWorkerImpls storageBenchWorkerImpls;
  MixedIOBenchWorkerImpls mixedIOBenchWorkerImpls;
  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);
  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);
  workerFactory.registerImpls("storagebench", storageBenchWorkerImpls);
  workerFactory.registerImpls("mixediobench", mixedIOBenchWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");
  taggerTracker.setFactory(&workerFactory, "storagebench", "tagger");
  senderTracker.setFactory(&workerFactory, "mapreduce", "kv_pair_buf_sender");
  receiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");
  demuxTracker.setFactory(&workerFactory, "mixediobench", "demux");
  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  // Inject dependencies into factories
  FilenameToStreamIDMap inputFilenameToStreamIDMap;
  workerFactory.addDependency(
    "filename_to_stream_id_map", &inputFilenameToStreamIDMap);
  workerFactory.addDependency("output_disk_list", &outputDiskList);

  workerFactory.addDependency("sender", "sockets", &senderSockets);
  workerFactory.addDependency("receiver", "sockets", &receiverSockets);

  // Create workers
  trackers.createWorkers();

  StatusPrinter::flush();

  // Start everything going
  Timer phaseTimer;
  phaseTimer.start();

  // kick off the source stages
  trackers.spawn();

  trackers.waitForWorkersToFinish();

  StatusPrinter::add("All benchmark workers are finished");
  StatusPrinter::flush();

  // All workers have finished

  phaseTimer.stop();

  phaseStatLogger.logDatum("phase_runtime", phaseTimer);

  delete memoryAllocator;

  // Destroy workers
  trackers.destroyWorkers();

  StatusPrinter::add("Phase 1 complete");
  StatusPrinter::flush();
}

int main(int argc, char** argv) {
  StatLogger* mainLogger = new StatLogger("storagebench");

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

  logStartTime();

  params.add<std::string>("COORDINATOR_CLIENT", "debug");

  limitMemorySize(params);

  executeBenchmark(&params);

  dumpParams(params);

  totalTimer.stop();

  ResourceMonitor::teardown();

  IntervalStatLogger::teardown();

  mainLogger->logDatum("total_runtime", totalTimer);

  // Delete logger so that it deregisters
  delete mainLogger;

  StatWriter::teardown();

  StatusPrinter::teardown();

  return 0;
}
