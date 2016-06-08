#ifndef TRITONSORT_RESOURCE_SCHEDULER_TEST_H
#define TRITONSORT_RESOURCE_SCHEDULER_TEST_H

#include <pthread.h>
#include "gtest.h"

#include "core/FCFSPolicy.h"
#include "core/MLFQPolicy.h"

class ResourceScheduler;

class ResourceSchedulerTest : public ::testing::Test {
public:
  void SetUp();
  void TearDown();

protected:
  FCFSPolicy FCFS;
  MLFQPolicy MLFQ;
  pthread_mutex_t sharedLock;
  ResourceScheduler* FCFSSchedulerWithoutCookies;
  ResourceScheduler* MLFQSchedulerWithCookies;
  SchedulerPolicy::Request* NULL_REQUEST;
};

#endif // TRITONSORT_RESOURCE_SCHEDULER_TEST_H
