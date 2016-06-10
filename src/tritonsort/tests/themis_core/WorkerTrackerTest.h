#ifndef TRITONSORT_WORKER_TRACKER_TEST_H
#define TRITONSORT_WORKER_TRACKER_TEST_H

#include "googletest.h"

#include "core/WorkerTracker.h"

class MockWorkerTracker;
class SimpleMemoryAllocator;

class WorkerTrackerTest : public ::testing::Test {
protected:
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
