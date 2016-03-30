#include <iostream>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>

#include "benchmarks/mallocbench/workers/mallocworker/MallocWorker.h"
#include "benchmarks/mallocbench/common/MemoryManager.h"
#include "common/MainUtils.h"
#include "core/Params.h"
#include "core/StatWriter.h"
#include "core/StatusPrinter.h"
#include "core/IntervalStatLogger.h"

int main(int argc, char** argv) {
  uint64_t randomSeed = Timer::posixTimeInMicros() * getpid();
  srand(randomSeed);

  signal(SIGSEGV, sigsegvHandler);

  if (argc == 1 ||
      (argc == 2 && (!strcmp(argv[1], "-h") ||
                     !strcmp(argv[1], "-help") ||
                     !strcmp(argv[1], "--help")))) {
    std::cerr << "Usage: " << argv[0]
              << " [params file] -param1 val 1 -param2 val2 ..."
              << std::endl << std::endl
              << "Required parameters:" << std::endl
              << "LOG_DIR = path to log directory" << std::endl
              << "ENABLE_STAT_WRITER = use stat writer?" << std::endl
              << "STAT_WRITER_DRAIN_INTERVAL_MICROS = us to drain" << std::endl
              << "STAT_POLL_INTERVAL = us to measure block queue length"
              << std::endl
              << "VERBOSE = print extra information? (1/0)" << std::endl
              << "WAKE_POLICY = specify when blocked threads wake up "
              << "(ASAP, FIFO, MLFQ_ASAP)" << std::endl
              << "CAPACITY = capacity of memory manager in bytes" << std::endl
              << "CHECKOUTS = the number of buffer checkouts a thread must make"
              << std::endl
              << "BUFFER_SIZE = the size of a buffer to checkout" << std::endl
              << "THREADS = number of threads accessing memory manager"
              << std::endl
              << "WORK_TIME = the average time in us a worker holds a buffer"
              << std::endl
              << "WORK_TIME_VARIABILITY = % deviation from the work time"
              << std::endl
              << "IDLE_TIME = the average time in us a worker pauses after "
              << "returning a buffer before it grabs another" << std::endl
              << "IDLE_TIME_VARIABILITY = % deviation from the idle time"
              << std::endl;
    exit(1);
  }

  Params params;
  params.parseCommandLine(argc, argv);

  params.add<std::string>("CHANNEL_STATUS_HEADER", "STATUS");
  params.add<std::string>("CHANNEL_STATISTIC_HEADER", "STATISTIC");
  params.add<std::string>("CHANNEL_PARAM_HEADER", "PARAM");

  StatusPrinter::init(&params);
  StatusPrinter::spawn();

  StatWriter::init(params);
  StatWriter::spawn();

  StatWriter::setCurrentPhaseName("malloc_bench");

  IntervalStatLogger::init(&params);
  IntervalStatLogger::spawn();

  // The actual benchmark
  const uint64_t CAPACITY = params.get<uint64_t>("CAPACITY");
  const bool VERBOSE = params.get<bool>("VERBOSE");
  const uint64_t CHECKOUTS = params.get<uint64_t>("CHECKOUTS");
  const uint64_t BUFFER_SIZE = params.get<uint64_t>("BUFFER_SIZE");
  const uint64_t THREADS = params.get<uint64_t>("THREADS");
  const uint64_t WORK_TIME = params.get<uint64_t>("WORK_TIME");
  const uint64_t WORK_TIME_VARIABILITY =
    params.get<uint64_t>("WORK_TIME_VARIABILITY");
  const uint64_t IDLE_TIME = params.get<uint64_t>("IDLE_TIME");
  const uint64_t IDLE_TIME_VARIABILITY =
    params.get<uint64_t>("IDLE_TIME_VARIABILITY");

  const std::string& WAKE_POLICY = params.get<std::string>("WAKE_POLICY");
  MemoryManager::WakePolicy wakePolicy = MemoryManager::ASAP;
  if (!WAKE_POLICY.compare("ASAP")) {
    wakePolicy = MemoryManager::ASAP;
  } else if (!WAKE_POLICY.compare("FIFO")) {
    wakePolicy = MemoryManager::FIFO;
  } else if (!WAKE_POLICY.compare("MLFQ_ASAP")) {
    wakePolicy = MemoryManager::MLFQ_ASAP;
  } else {
    ABORT("Unknown wake policy %s. Use ASAP, FIFO, or MLFQ_ASAP",
          WAKE_POLICY.c_str());
  }

  MallocWorker** workers = new MallocWorker*[THREADS];

  // Create a memory manager responsible for CAPACITY bytes
  MemoryManager* memoryManager = new MemoryManager(CAPACITY, VERBOSE,
                                                   wakePolicy);

  // Create and spawn a number of threads that each try to get a buffer a number
  // of times
  for (uint64_t workerID = 0; workerID < THREADS; ++workerID) {
    workers[workerID] = new MallocWorker(workerID, "mallocworker",
                                         *memoryManager, CHECKOUTS, BUFFER_SIZE,
                                         WORK_TIME, WORK_TIME_VARIABILITY,
                                         IDLE_TIME, IDLE_TIME_VARIABILITY,
                                         CAPACITY);
    workers[workerID]->spawn();
  }

  // Wait for each thread to finish
  for (uint64_t workerID = 0; workerID < THREADS; ++workerID) {
    workers[workerID]->join();
    delete workers[workerID];
  }

  // Cleanup
  IntervalStatLogger::teardown();

  delete memoryManager;

  delete[] workers;

  StatWriter::teardown();

  StatusPrinter::flush();
  StatusPrinter::teardown();

  return 0;
}
