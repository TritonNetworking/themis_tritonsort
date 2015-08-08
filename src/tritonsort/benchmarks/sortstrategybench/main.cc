#include <boost/filesystem.hpp>
#include <iostream>
#include <json/value.h>
#include <json/writer.h>
#include <limits>
#include <signal.h>
#include <sstream>
#include <stdlib.h>

#include "config.h"

#include <jemalloc/jemalloc.h>
const char* malloc_conf = "narenas:1,lg_chunk:20";

#include "benchmarks/sortstrategybench/workers/SortStrategyBenchWorkerImpls.h"
#include "common/MainUtils.h"
#include "core/AbortingDeadlockResolver.h"
#include "core/CPUAffinitySetter.h"
#include "core/DefaultAllocatorPolicy.h"
#include "core/File.h"
#include "core/MemoryAllocator.h"
#include "core/Params.h"
#include "core/StatusPrinter.h"
#include "core/Timer.h"
#include "core/TrackerSet.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/WorkerTracker.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/workers/MapReduceWorkerImpls.h"

void benchmark(Params& params) {
  // Create input files.
  std::string vargensort = params.get<std::string>("VARGENSORT_BIN");

  uint64_t maxKeyLength = params.get<uint64_t>("MAX_KEY_LENGTH");
  uint64_t maxValueLength = params.get<uint64_t>("MAX_VALUE_LENGTH");
  double paretoA = params.get<double>("PARETO_A");
  uint64_t paretoB = params.get<uint64_t>("PARETO_B");
  uint64_t bytes = params.get<uint64_t>("BYTES");

  std::string disk = params.get<std::string>("DISK");
  uint64_t fileID = params.get<uint64_t>("FILE_ID");

  // Create the file with vargensort.
  boost::filesystem::path file(
    boost::filesystem::path(disk) /
    ("input_" + boost::lexical_cast<std::string>(fileID)));

  std::ostringstream vargensortCommand;
  vargensortCommand
    << vargensort << " -maxKeyLen " << maxKeyLength << " -maxValLen "
    << maxValueLength << " -pareto_a " << paretoA << " -pareto_b " << paretoB
    << " " << file.string() << " pareto " << bytes;
  std::cout << vargensortCommand.str().c_str() << std::endl;
  int status = system(vargensortCommand.str().c_str());
  ABORT_IF(status != 0, "system() exited with status %d", status);

  // Run pipeline:
  std::string phaseName = "phase";
  WorkerTracker readerTracker(params, phaseName, "reader");
  WorkerTracker validatorTracker(params, phaseName, "validator");
  WorkerTracker timestamperStartTracker(
    params, phaseName, "timestamper_start");
  WorkerTracker sorterTracker(params, phaseName, "sorter");
  WorkerTracker timestamperStopTracker(params, phaseName, "timestamper_stop");
  WorkerTracker writerTracker(params, phaseName, "writer");

  TrackerSet trackers;
  trackers.addTracker(&readerTracker, true);
  trackers.addTracker(&validatorTracker);
  trackers.addTracker(&timestamperStartTracker);
  trackers.addTracker(&sorterTracker);
  trackers.addTracker(&timestamperStopTracker);
  trackers.addTracker(&writerTracker);

  readerTracker.isSourceTracker();
  readerTracker.addDownstreamTracker(&validatorTracker);
  validatorTracker.addDownstreamTracker(&timestamperStartTracker);
  timestamperStartTracker.addDownstreamTracker(&sorterTracker);
  sorterTracker.addDownstreamTracker(&timestamperStopTracker);
  timestamperStopTracker.addDownstreamTracker(&writerTracker);

  // === More boilerplate that depends on the pipeline
  DefaultAllocatorPolicy allocatorPolicy(trackers);
  AbortingDeadlockResolver deadlockResolver;
  MemoryAllocator memoryAllocator(
    params.get<uint64_t>("ALLOCATOR_CAPACITY"),
    params.get<uint64_t>("ALLOCATOR_FRAGMENTATION_SLEEP"), allocatorPolicy,
    deadlockResolver);

  MapReduceWorkerImpls mapReduceWorkerImpls;
  SortStrategyBenchWorkerImpls sortStrategyBenchWorkerImpls;

  CPUAffinitySetter cpuAffinitySetter(params, phaseName);
  WorkerFactory workerFactory(params, cpuAffinitySetter, memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);
  workerFactory.registerImpls(
    "sortstrategybench", sortStrategyBenchWorkerImpls);

  readerTracker.setFactory(&workerFactory, "mapreduce", "reader");

  validatorTracker.setFactory(&workerFactory, "sortstrategybench", "validator");

  timestamperStartTracker.setFactory(
    &workerFactory, "sortstrategybench", "timestamper");

  sorterTracker.setFactory(&workerFactory, "mapreduce", "sorter");

  timestamperStopTracker.setFactory(
    &workerFactory, "sortstrategybench", "timestamper");

  writerTracker.setFactory(&workerFactory, "mapreduce", "writer");

  // ===

  std::vector<std::string> nodes;
  nodes.push_back("localhost");

  // Inject dependencies into factory
  uint64_t startTimestamp = std::numeric_limits<uint64_t>::max();
  uint64_t stopTimestamp = std::numeric_limits<uint64_t>::max();
  uint64_t scratchSize = std::numeric_limits<uint64_t>::max();
  workerFactory.addDependency(
    "timestamper_start", "timestamp", &startTimestamp);
  workerFactory.addDependency("timestamper_stop", "timestamp", &stopTimestamp);
  workerFactory.addDependency(
    "scratch_size", &scratchSize);

  // Instantiate workers
  trackers.createWorkers();

  // Add input files to readers
  std::set<uint64_t> jobIDs;
  jobIDs.insert(0xdeadbeef);
  ReadRequest* workUnit = new ReadRequest(
    jobIDs, ReadRequest::FILE, "localhost", 0, file.string(), 0,
    getFileSize(file.string().c_str()), 0);
  readerTracker.addWorkUnit(workUnit);

  // Spawn workers
  trackers.spawn();

  // Start deadlock checker thread (which should abort if the system deadlocks)
  memoryAllocator.spawnDeadlockChecker();

  // Run pipeline to completion
  trackers.waitForWorkersToFinish();

  // Cleanup
  memoryAllocator.stopDeadlockChecker();
  trackers.destroyWorkers();

  // Construct JSON object corresponding to run statistics.
  Json::Value stats;
  stats["file_id"] = Json::UInt64(fileID);
  stats["sort_strategy"] = params.get<std::string>("SORT_STRATEGY");
  stats["max_key_length"] = Json::UInt64(maxKeyLength);
  stats["max_value_length"] = Json::UInt64(maxValueLength);
  stats["pareto_a"] = paretoA;
  stats["pareto_b"] = Json::UInt64(paretoB);
  stats["bytes"] = Json::UInt64(bytes);
  stats["scratch_size"] = Json::UInt64(scratchSize);
  if (startTimestamp != std::numeric_limits<uint64_t>::max()) {
    stats["time"] = Json::UInt64(stopTimestamp - startTimestamp);
  }

  // Write the JSON object out to a file
  boost::filesystem::path statsFilePath(
    boost::filesystem::path(disk) /
    ("stats_" + boost::lexical_cast<std::string>(fileID) + ".json"));
  File statsFile(statsFilePath.string());

  statsFile.open(File::WRITE, true);

  Json::StyledWriter writer;
  std::string statsString = writer.write(stats);
  statsFile.write(statsString);

  statsFile.close();
}

int main(int argc, char** argv) {
  uint64_t randomSeed = Timer::posixTimeInMicros() * getpid();
  srand(randomSeed);
  srand48(randomSeed);

  signal(SIGBUS, TritonSortAssertions::dumpStack);
  // Gather StackTrace for segfault
  signal(SIGSEGV, sigsegvHandler);

  Params params;
  params.parseCommandLine(argc, argv);

  // Set benchmark constant params
  params.add<uint64_t>("NUM_WORKERS.phase.reader", 1);
  params.add<uint64_t>("NUM_WORKERS.phase.validator", 1);
  params.add<uint64_t>("NUM_WORKERS.phase.timestamper_start", 1);
  params.add<uint64_t>("NUM_WORKERS.phase.sorter", 1);
  params.add<uint64_t>("NUM_WORKERS.phase.timestamper_stop", 1);
  params.add<uint64_t>("NUM_WORKERS.phase.writer", 1);

  params.add<std::string>("READER_IMPL", "WholeFileReader");
  params.add<std::string>("VALIDATOR_IMPL", "Validator");
  params.add<std::string>("TIMESTAMPER_IMPL", "Timestamper");
  params.add<std::string>("SORTER_IMPL", "Sorter");
  params.add<std::string>("WRITER_IMPL", "Writer");

  params.add<uint64_t>("MYPEERID", 0);

  // Log to /tmp since we don't care
  params.add<std::string>("LOG_DIR", "/tmp");

  // Spawn non-worker threads.
  StatusPrinter::init(&params);
  StatusPrinter::spawn();

  benchmark(params);

  StatusPrinter::teardown();

  return 0;
}
