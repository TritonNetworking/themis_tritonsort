#ifndef _TRITONSORT_TEST_DEFAULT_ALLOCATOR_POLICY_TEST_H
#define _TRITONSORT_TEST_DEFAULT_ALLOCATOR_POLICY_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocatorPolicy.h"
#include "core/TrackerSet.h"
#include "tests/themis_core/MockWorkerTracker.h"

/**
   \TODO(MC): Add tests for more complicated graphs when we support multiple
   nextTrackers
*/
class DefaultAllocatorPolicyTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( DefaultAllocatorPolicyTest );
  CPPUNIT_TEST( testSingleAllocation );
  CPPUNIT_TEST( testBlockWhenOutOfMemory );
  CPPUNIT_TEST( testPriorityQueues );
  CPPUNIT_TEST_SUITE_END();

public:
  DefaultAllocatorPolicyTest();
  void testSingleAllocation();
  void testBlockWhenOutOfMemory();
  void testPriorityQueues();

private:
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
