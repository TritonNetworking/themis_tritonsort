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

TEST_F(MemoryAllocatorTests, testLargeMemoryAllocationFails) {
  FCFSMemoryAllocatorPolicy policy;
  NopDeadlockResolver deadlockResolver;
  MemoryAllocator allocator(1000000, 500000, policy, deadlockResolver);

  DummyWorker dummyWorker(0, "test");

  uint64_t callerID = allocator.registerCaller(dummyWorker);

  MemoryAllocationContext context(callerID, 4000000);

  ASSERT_THROW(allocator.allocate(context), AssertionFailedException);
}
