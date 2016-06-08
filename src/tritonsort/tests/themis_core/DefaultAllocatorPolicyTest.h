#ifndef _TRITONSORT_TEST_DEFAULT_ALLOCATOR_POLICY_TEST_H
#define _TRITONSORT_TEST_DEFAULT_ALLOCATOR_POLICY_TEST_H

#include "gtest.h"

#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocatorPolicy.h"
#include "core/TrackerSet.h"
#include "tests/themis_core/MockWorkerTracker.h"

/**
   \TODO(MC): Add tests for more complicated graphs when we support multiple
   nextTrackers
*/
class DefaultAllocatorPolicyTest : public ::testing::Test {
public:
  DefaultAllocatorPolicyTest();

protected:
  TrackerSet singleTrackerSet;
  TrackerSet chainTrackerSet;

  MockWorkerTracker singleTracker;
  MockWorkerTracker chainTracker1;
  MockWorkerTracker chainTracker2;
  MockWorkerTracker chainTracker3;

  MemoryAllocationContext context100;
  MemoryAllocationContext context200;
  MemoryAllocationContext context300;
  MemoryAllocationContext context1000;

  MemoryAllocatorPolicy::Request* NULL_REQUEST;
};

#endif // _TRITONSORT_TEST_DEFAULT_ALLOCATOR_POLICY_TEST_H
