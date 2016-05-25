#include "BatchRunnableTest.h"
#include "MockBatchRunnable.h"
#include "TestWorkerImpls.h"
#include "common/DummyWorkUnit.h"
#include "common/SimpleMemoryAllocator.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "core/WorkerBarrier.h"
#include "core/WorkerFactory.h"
#include "core/WorkerTracker.h"

TEST_F(BatchRunnableTest, testBasic) {
  Params params;
  params.add<uint64_t>("CORES_PER_NODE", 16);
  params.add<uint64_t>("NUM_WORKERS.test_phase.mock_batch", 1);
  params.add<std::string>(
    "WORKER_IMPLS.test_phase.mock_batch", "MockBatchRunnable");

  WorkerTracker batchRunnableTracker(params, "test_phase", "mock_batch");

  CPUAffinitySetter cpuAffinitySetter(params, "test_phase");

  TestWorkerImpls testWorkerImpls;

  SimpleMemoryAllocator memoryAllocator;

  WorkerFactory factory(params, cpuAffinitySetter, memoryAllocator);

  factory.registerImpls("test_workers", testWorkerImpls);

  for (uint64_t i = 0; i < 200; i++) {
    batchRunnableTracker.addWorkUnit(new DummyWorkUnit());
  }

  batchRunnableTracker.setFactory(
    &factory, "test_workers");

  batchRunnableTracker.isSourceTracker();
  batchRunnableTracker.createWorkers();

  MockBatchRunnable* worker = dynamic_cast<MockBatchRunnable*>(
    batchRunnableTracker.getWorkers()[0]);

  EXPECT_TRUE(worker != NULL);

  batchRunnableTracker.spawn();

  batchRunnableTracker.waitForWorkersToFinish();

  EXPECT_EQ((uint64_t) 200, worker->getNumWorkUnitsProcessed());
  batchRunnableTracker.destroyWorkers();
}
