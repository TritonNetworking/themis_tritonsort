#include <iostream>

#include "MemoryAllocatorTests.h"
#include "common/DummyWorker.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocator.h"
#include "core/Params.h"
#include "core/WorkerTracker.h"
#include "tests/themis_core/FCFSMemoryAllocatorPolicy.h"
#include "tests/themis_core/MemoryEatingWorker.h"
#include "tests/themis_core/MockWorkerTracker.h"
#include "tests/themis_core/NopDeadlockResolver.h"
#include "tests/themis_core/TestWorkerImpls.h"

void MemoryAllocatorTests::testLargeMemoryAllocationFails() {
  FCFSMemoryAllocatorPolicy policy;
  NopDeadlockResolver deadlockResolver;
  MemoryAllocator allocator(1000000, 500000, policy, deadlockResolver);

  DummyWorker dummyWorker(0, "test");

  uint64_t callerID = allocator.registerCaller(dummyWorker);

  MemoryAllocationContext context(callerID, 4000000);

  CPPUNIT_ASSERT_THROW(allocator.allocate(context), AssertionFailedException);
}

void MemoryAllocatorTests::testDeadlockDetection() {
  Params params;
  params.add<uint64_t>("CORES_PER_NODE", 1);
  params.add<std::string>(
    "WORKER_IMPLS.test.memory_eater", "MemoryEatingWorker");
  params.add<uint64_t>("NUM_WORKERS.test.memory_eater", 1);

  FCFSMemoryAllocatorPolicy policy;
  NopDeadlockResolver deadlockResolver;
  MemoryAllocator allocator(1000000, 500000, policy, deadlockResolver);
  CPUAffinitySetter cpuAffinitySetter(params, "test");
  TestWorkerImpls workerImpls;

  WorkerTracker tracker(params, "test", "memory_eater");
  MockWorkerTracker sinkTracker("sink");

  WorkerFactory factory(params, cpuAffinitySetter, allocator);
  factory.registerImpls("tests", workerImpls);

  // The memory eater should allocate more memory than is available on the
  // fourth allocation and deadlock
  tracker.addWorkUnit(new UInt64Resource(400000));
  tracker.addWorkUnit(new UInt64Resource(500000));
  tracker.addWorkUnit(new UInt64Resource(70000));
  tracker.addWorkUnit(new UInt64Resource(300000));

  tracker.isSourceTracker();
  tracker.setFactory(&factory, "tests");
  tracker.addDownstreamTracker(&sinkTracker);

  tracker.createWorkers();
  tracker.spawn();

  // If the deadlock checker were running, it should be able to detect the
  // deadlock; give it 5 seconds to detect

  bool deadlockDetected = false;

  for (uint64_t i = 0; !deadlockDetected && i < 5000; i++) {
    deadlockDetected = allocator.detectAndResolveDeadlocks();
    usleep(1000);
  }

  CPPUNIT_ASSERT_EQUAL(true, deadlockDetected);

  tracker.waitForWorkersToFinish();

  tracker.destroyWorkers();
}
