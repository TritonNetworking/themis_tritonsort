#ifndef TRITONSORT_WORKER_TRACKER_TEST_H
#define TRITONSORT_WORKER_TRACKER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "core/WorkerTracker.h"

class SimpleMemoryAllocator;

class WorkerTrackerTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( WorkerTrackerTest );
  CPPUNIT_TEST( testMultiSource );
  CPPUNIT_TEST( testMultiDestination );
  CPPUNIT_TEST( testDefaultWorkQueueingPolicy );
  CPPUNIT_TEST_SUITE_END();

public:
  void testMultiSource();
  void testMultiDestination();
  void testDefaultWorkQueueingPolicy();

private:
  void checkQueueSizes(
    WorkerTracker::WorkerVector& workers,
    std::vector<uint64_t>& expectedQueueSizes);
  void clearAllWorkerQueues(WorkerTracker::WorkerVector& workers);
  void checkSinkTrackerQueue(
    MockWorkerTracker& tracker, uint64_t size,
    const std::string& expectedString);
  WorkerTracker* setupTrackerForQueueingTests(
    CPUAffinitySetter*& cpuAffinitySetter,
    SimpleMemoryAllocator*& memoryAllocator);
};

#endif // TRITONSORT_WORKER_TRACKER_TEST_H
