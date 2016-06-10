#include <signal.h>
#include <stdlib.h>

#include "tritonsort/config.h"

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
const char* malloc_conf = "lg_chunk:20,lg_tcache_gc_sweep:30";
#endif // USE_JEMALLOC

#include "benchmarks/networkbench/workers/NetworkBenchWorkerImpls.h"
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
#include "mapreduce/common/queueing/MapReduceWorkQueueingPolicyFactory.h"
#include "mapreduce/workers/MapReduceWorkerImpls.h"

void executeBenchmark(Params* params, IPList& peers) {
  std::string phaseName("networkbench");

  StatLogger phaseStatLogger(phaseName);
  StatWriter::setCurrentPhaseName(phaseName);

  // Open all-to-all TCP sockets.
  SocketArray senderSockets;
  SocketArray receiverSockets;
  openAllSockets(
    params->get<std::string>("RECEIVER_PORT"), peers.size(), *params, phaseName,
    "sender", peers, peers, senderSockets, receiverSockets);

  // Set up trackers and plumb workers together
  MapReduceWorkQueueingPolicyFactory queueingPolicyFactory;
  WorkerTracker generatorTracker(
    *params, phaseName, "generator", &queueingPolicyFactory);
  QuotaEnforcingWorkerTracker senderTracker(
    *params, phaseName, "sender", &queueingPolicyFactory);

  WorkerTracker receiverTracker(
    *params, phaseName, "receiver", &queueingPolicyFactory);
  WorkerTracker sinkTracker(
    *params, phaseName, "sink", &queueingPolicyFactory);

  MemoryQuota generatorQuota(
    "generator_quota",
    params->get<uint64_t>("MEMORY_QUOTAS.networkbench.generator"));
  senderTracker.addProducerQuota(generatorQuota);
  senderTracker.addConsumerQuota(generatorQuota);

  TrackerSet trackers;
  trackers.addTracker(&generatorTracker, true);
  trackers.addTracker(&senderTracker);

  generatorTracker.isSourceTracker();
  generatorTracker.addDownstreamTracker(&senderTracker);

  trackers.addTracker(&receiverTracker, true);
  trackers.addTracker(&sinkTracker);

  receiverTracker.isSourceTracker();
  receiverTracker.addDownstreamTracker(&sinkTracker);

  SimpleMemoryAllocator* memoryAllocator = new SimpleMemoryAllocator();

  MapReduceWorkerImpls mapReduceWorkerImpls;
  NetworkBenchWorkerImpls networkBenchWorkerImpls;
  CPUAffinitySetter cpuAffinitySetter(*params, phaseName);
  WorkerFactory workerFactory(*params, cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("mapreduce", mapReduceWorkerImpls);
  workerFactory.registerImpls("networkbench", networkBenchWorkerImpls);

  generatorTracker.setFactory(&workerFactory, "networkbench", "generator");
  senderTracker.setFactory(&workerFactory, "mapreduce", "kv_pair_buf_sender");
  receiverTracker.setFactory(
    &workerFactory, "mapreduce", "kv_pair_buf_receiver");
  // Sink is going to be a SinkMapper
  sinkTracker.setFactory(
    &workerFactory, "mapreduce", "mapper");

  // Inject dependencies into factories
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
  StatLogger* mainLogger = new StatLogger("networkbench");

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

  // Parse peer list.
  IPList peers;
  parsePeerList(params, peers);

  uint64_t partitionGroupsPerNode =
    params.get<uint64_t>("BENCHMARK_PARTITION_GROUPS_PER_NODE");

  params.add<uint64_t>("PARTITION_GROUPS_PER_NODE", partitionGroupsPerNode);
  params.add<uint64_t>(
    "NUM_PARTITION_GROUPS",
    partitionGroupsPerNode * params.get<uint64_t>("NUM_PEERS"));

  params.add<std::string>("COORDINATOR_CLIENT", "none");

  limitMemorySize(params);

  executeBenchmark(&params, peers);

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
