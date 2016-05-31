#include <boost/math/common_factor.hpp>
#include <signal.h>
#include <stdlib.h>

#include "config.h"

#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30";

#include "benchmarks/storagebench/workers/StorageBenchWorkerImpls.h"
#include "common/AlignmentUtils.h"
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
  std::string phaseName("storagebench");

  StatLogger phaseStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  StringList inputDiskList;
  std::string inputDiskListParam = "INPUT_DISK_LIST." + phaseName;
  getDiskList(inputDiskList, inputDiskListParam, params);
  uint64_t numInputDisks = inputDiskList.size();
  params->add<uint64_t>("NUM_INPUT_DISKS", numInputDisks);
  StringList outputDiskList;
  std::string outputDiskListParam = "OUTPUT_DISK_LIST." + phaseName;
  getDiskList(outputDiskList, outputDiskListParam, params);
  uint64_t numOutputDisks = inputDiskList.size();

  // Adjust partition size so we get roughly the correct amount of data.
  uint64_t dataSize = params->getv<uint64_t>(
    "BENCHMARK_DATA_SIZE_PER_NODE.%s", phaseName.c_str());
  uint64_t partitionSize = params->get<uint64_t>("PARTITION_SIZE");
  uint64_t numPartitionsPerNode = dataSize / partitionSize;

  // Round up to a multiple of the number of input and output disks.
  uint64_t partitionMultiple =
    boost::math::lcm(numInputDisks, numOutputDisks);
  numPartitionsPerNode = roundUp(numPartitionsPerNode, partitionMultiple);

  // New partition size might be slightly larger than what the user wanted but
  // will more evenly divide the data size.
  params->add<uint64_t>("PARTITION_SIZE", dataSize / numPartitionsPerNode);

  // Even if the user is running this benchmark on multiple nodes, each node is
  // considered its own cluster, so act as if there is only 1 peer.
  params->add<uint64_t>("NUM_PEERS", 1);
  params->add<uint64_t>("MYPEERID", 0);
  // We won't be needing the fault tolerance part of the writer for this
  // benchmark.
  params->add<std::string>("MYIPADDRESS", "NULL");
  params->add<uint64_t>("NUM_PARTITIONS", numPartitionsPerNode);

  bool reading = params->get<bool>("READ");
  bool writing = params->get<bool>("WRITE");
  ABORT_IF(!reading && !writing, "Must either read, write, or both.");

  // Set alignment parameters:
  uint64_t alignmentMultiple = params->get<uint64_t>("ALIGNMENT_MULTIPLE");
  if (params->getv<bool>("DIRECT_IO.%s.reader", phaseName.c_str())) {
    // Reader should align buffers.
    params->add<uint64_t>(
      "ALIGNMENT." + phaseName + ".reader", alignmentMultiple);
  }

  if (params->getv<bool>("DIRECT_IO.%s.writer", phaseName.c_str())) {
    if (reading) {
      // Reader should align buffers so that the writer can write them.
      params->add<uint64_t>(
        "ALIGNMENT." + phaseName + ".reader", alignmentMultiple);
    } else {
      // Generator should align buffers for the writer.
      params->add<uint64_t>(
        "ALIGNMENT." + phaseName + ".generator", alignmentMultiple);
    }

    // Writer needs to know that it should write with alignment.
    params->add<uint64_t>(
        "ALIGNMENT." + phaseName + ".writer", alignmentMultiple);
  }

  // The reader and generator can use caching allocators since they allocate
  // fixed size buffers.
  if (params->contains("CACHED_MEMORY.storagebench.reader")) {
    params->add<bool>("CACHING_ALLOCATOR.storagebench.reader", true);
  }

  if (params->contains("CACHED_MEMORY.storagebench.generator")) {
    params->add<bool>("CACHING_ALLOCATOR.storagebench.generator", true);
  }

  StringList* writerDiskList = NULL;

  // Compute disks per reader/writer
  uint64_t numReaders = params->getv<uint64_t>(
    "NUM_WORKERS.%s.reader", phaseName.c_str());
  params->add<uint64_t>(
    "DISKS_PER_WORKER." + phaseName + ".reader",
    numInputDisks / numReaders);
  uint64_t numWriters = params->getv<uint64_t>(
    "NUM_WORKERS.%s.writer", phaseName.c_str());

  if (reading) {
    // Since we're reading input files, a writer, if it exists, will be writing
    // output files.
    params->add<uint64_t>(
      "DISKS_PER_WORKER." + phaseName + ".writer",
      numOutputDisks / numWriters);
    params->add<uint64_t>(
      "NUM_OUTPUT_DISKS." + phaseName, numOutputDisks);

    writerDiskList = &outputDiskList;
  } else {
    // We're not reading, so write input files.
    params->add<uint64_t>(
      "DISKS_PER_WORKER." + phaseName + ".writer",
      numInputDisks / numWriters);
    params->add<uint64_t>(
      "NUM_OUTPUT_DISKS." + phaseName, numInputDisks);

    writerDiskList = &inputDiskList;
  }

  // Set up trackers and plumb workers together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;
  WorkerTracker readerTracker(
    *params, phaseName, "reader", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker taggerTracker(
    *params, phaseName, "tagger", &queueingPolicyFactory);
  WorkerTracker sinkTracker(
    *params, phaseName, "sink", &queueingPolicyFactory);
  WorkerTracker generatorTracker(
    *params, phaseName, "generator", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker writerTracker(
    *params, phaseName, "writer", &queueingPolicyFactory);

  MemoryQuota readerQuota(
    "reader_quota", params->get<uint64_t>("MEMORY_QUOTAS.storagebench.reader"));
  taggerTracker.addProducerQuota(readerQuota);
  taggerTracker.addConsumerQuota(readerQuota);
  // Technically this is a quota on whatever comes before the writer, but
  // since the pipeline is configuration-dependent, we'll call it the writer
  // quota.
  MemoryQuota writerQuota(
    "writer_quota", params->get<uint64_t>("MEMORY_QUOTAS.storagebench.writer"));
  writerTracker.addProducerQuota(writerQuota);
  writerTracker.addConsumerQuota(writerQuota);

  TrackerSet trackers;
  // We're either reading, writing, or both.
  if (reading && writing) {
    // Reader -> Tagger -> Writer
    trackers.addTracker(&readerTracker, true);
    trackers.addTracker(&taggerTracker);
    trackers.addTracker(&writerTracker);

    readerTracker.isSourceTracker();
    readerTracker.addDownstreamTracker(&taggerTracker);
    taggerTracker.addDownstreamTracker(&writerTracker);
  } else if (reading) {
    // Reader -> Sink
    trackers.addTracker(&readerTracker, true);
    trackers.addTracker(&sinkTracker);

    readerTracker.isSourceTracker();
    readerTracker.addDownstreamTracker(&sinkTracker);
  } else {
    // Writer
    trackers.addTracker(&generatorTracker, true);
    trackers.addTracker(&writerTracker);

    generatorTracker.isSourceTracker();
    generatorTracker.addDownstreamTracker(&writerTracker);
  }

  // Populate initial work units depending on whether or not we're reading
  // actual files. If we're not, the generator will take care of this for us.
  if (reading) {
    // Read files.
    loadReadRequests(readerTracker, *params, phaseName);
  }

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;
  StorageBenchWorkerImpls storageBenchWorkerImpls;
  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);
  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);
  workerFactory.registerImpls("storagebench", storageBenchWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");
  taggerTracker.setFactory(&workerFactory, "storagebench", "tagger");
  // Sink is going to be a SinkMapper
  sinkTracker.setFactory(&workerFactory, "mapreduce", "mapper");
  generatorTracker.setFactory(&workerFactory, "storagebench", "generator");
  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  // Inject dependencies into factories
  FilenameToStreamIDMap inputFilenameToStreamIDMap;
  workerFactory.addDependency(
    "filename_to_stream_id_map", &inputFilenameToStreamIDMap);
  workerFactory.addDependency("output_disk_list", writerDiskList);

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
  params.add<uint64_t>("NUM_PARTITION_GROUPS", 1);

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
