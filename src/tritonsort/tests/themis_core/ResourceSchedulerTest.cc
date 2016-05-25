#include "ResourceSchedulerTest.h"
#include "core/ResourceScheduler.h"
#include "core/TritonSortAssert.h"

void ResourceSchedulerTest::SetUp() {
  // Create two 1GB schedulers:
  // The first uses FCFS without cookies, and the second uses MLFQ with cookies.
  const uint64_t OneGB = 1000000000;
  // Although a shared lock is passed to the schedulers, there is no need to use
  // it in the tests since there is only 1 thread and the scheduler aborts if it
  // blocks.
  FCFSSchedulerWithoutCookies = new ResourceScheduler(OneGB, FCFS, sharedLock,
                                                      false, true);
  MLFQSchedulerWithCookies = new ResourceScheduler(OneGB, MLFQ, sharedLock,
                                                   true, true);

  NULL_REQUEST = NULL;
}

void ResourceSchedulerTest::TearDown() {
  delete FCFSSchedulerWithoutCookies;
  delete MLFQSchedulerWithCookies;
}

TEST_F(ResourceSchedulerTest, testFCFSPolicyOrdering) {
  FCFSPolicy* FCFS = new FCFSPolicy();

  // Create some requests and verify the FIFO ordering
  SchedulerPolicy::Request request1(NULL, 0);
  SchedulerPolicy::Request request2(NULL, 0);
  SchedulerPolicy::Request request3(NULL, 0);

  // Add requests to the policy's queue
  FCFS->addRequest(request1);
  FCFS->addRequest(request2);
  FCFS->addRequest(request3);

  // Verify that the head of the queue can be scheduled.
  EXPECT_TRUE(FCFS->canScheduleRequest(request1));
  EXPECT_TRUE(!FCFS->canScheduleRequest(request2));
  EXPECT_TRUE(!FCFS->canScheduleRequest(request3));

  // Verify the head of the queue is schedulable - using the call that searches
  // for the next schedulable item.
  SchedulerPolicy::Request* next = FCFS->getNextSchedulableRequest(0);
  EXPECT_EQ(&request1, next);

  // Should be able to remove the head of the queue.
  ASSERT_NO_THROW(FCFS->removeRequest(request1));

  // Repeat above until queue is empty:
  // Check scheduability of requests in queues
  EXPECT_TRUE(FCFS->canScheduleRequest(request2));
  EXPECT_TRUE(!FCFS->canScheduleRequest(request3));

  // Remove the next request
  next = FCFS->getNextSchedulableRequest(0);
  EXPECT_EQ(&request2, next);
  ASSERT_NO_THROW(FCFS->removeRequest(request2));

  // Check schedulability of requests in queues
  EXPECT_TRUE(FCFS->canScheduleRequest(request3));

  // Remove the next request
  next = FCFS->getNextSchedulableRequest(0);
  EXPECT_EQ(&request3, next);
  ASSERT_NO_THROW(FCFS->removeRequest(request3));

  // Verify queues are empty
  next = FCFS->getNextSchedulableRequest(0);
  EXPECT_EQ(NULL_REQUEST, next);

  ASSERT_NO_THROW(delete FCFS);
}

TEST_F(ResourceSchedulerTest, testMLFQPolicyOrdering) {
  MLFQPolicy* MLFQ = new MLFQPolicy();

  // Give a fake 1minute usage time to the policy. This should set the average
  // usage time, and hence the escalation timeout, to 1 minute.
  const uint64_t oneMinute = 60000000;
  MLFQ->recordUseTime(oneMinute);

  // Create some requests and verify MLFQ low priority and high priority
  // queuing policies.
  SchedulerPolicy::Request request1(NULL, 400);
  SchedulerPolicy::Request request2(NULL, 200);
  SchedulerPolicy::Request request3(NULL, 300);

  // Add requests to the policy's queues
  MLFQ->addRequest(request1);
  MLFQ->addRequest(request2);
  MLFQ->addRequest(request3);

  // Verify that all requests are schedulable since they are in the low priority
  // queue, which is not FCFS.
  EXPECT_TRUE(MLFQ->canScheduleRequest(request1));
  EXPECT_TRUE(MLFQ->canScheduleRequest(request2));
  EXPECT_TRUE(MLFQ->canScheduleRequest(request3));

  // Verify that, given 250 available resources. The second request can be
  // scheduled, but not the other two.
  SchedulerPolicy::Request* next = MLFQ->getNextSchedulableRequest(250);
  EXPECT_EQ(&request2, next);
  ASSERT_NO_THROW(MLFQ->removeRequest(request2));

  EXPECT_TRUE(MLFQ->canScheduleRequest(request1));
  EXPECT_TRUE(MLFQ->canScheduleRequest(request3));

  next = MLFQ->getNextSchedulableRequest(250);
  EXPECT_EQ(NULL_REQUEST, next);

  // Verify that given 350 resources, the last request would be scheduled next.
  next = MLFQ->getNextSchedulableRequest(350);
  EXPECT_EQ(&request3, next);

  // Fake the timestamps on both of the outstanding requests to bump them into
  // the high priority queue.
  request1.timestamp -= 2 * oneMinute; // Roll back timestamp by 2 minutes
  request3.timestamp -= 2 * oneMinute;

  // Now no request should be scheduable with 350 resources since the high
  // priority queue is serviced FIFO with 400 being the head.
  next = MLFQ->getNextSchedulableRequest(350);
  EXPECT_EQ(NULL_REQUEST, next);

  EXPECT_TRUE(MLFQ->canScheduleRequest(request1));
  EXPECT_TRUE(!MLFQ->canScheduleRequest(request3));

  // Schedule both of the remaining requests.
  next = MLFQ->getNextSchedulableRequest(400);
  EXPECT_EQ(&request1, next);
  ASSERT_NO_THROW(MLFQ->removeRequest(request1));

  EXPECT_TRUE(MLFQ->canScheduleRequest(request3));

  next = MLFQ->getNextSchedulableRequest(300);
  EXPECT_EQ(&request3, next);
  ASSERT_NO_THROW(MLFQ->removeRequest(request3));

  // Queues should be empty
  next = MLFQ->getNextSchedulableRequest(1000);
  EXPECT_EQ(NULL_REQUEST, next);

  ASSERT_NO_THROW(delete MLFQ);
}

TEST_F(ResourceSchedulerTest, testScheduleSingleResourceWithoutCookies) {
  const uint64_t SevenHundredMB = 700000000;
  const void* caller = NULL;

  // Schedule 700MB without cookies.
  ASSERT_NO_THROW(
    FCFSSchedulerWithoutCookies->schedule(SevenHundredMB, caller));

  // Release the resources
  ASSERT_NO_THROW(
    FCFSSchedulerWithoutCookies->release(SevenHundredMB));
}

TEST_F(ResourceSchedulerTest, testScheduleSingleResourceWithCookies) {
  const uint64_t SevenHundredMB = 700000000;
  const void* caller = NULL;

  // Schedule 700MB with cookies.
  const void* cookie = NULL;
  ASSERT_NO_THROW(
    cookie = MLFQSchedulerWithCookies->scheduleWithCookie(SevenHundredMB,
                                                          caller));

  // Release the resources
  ASSERT_NO_THROW(
    MLFQSchedulerWithCookies->releaseWithCookie(cookie));
}

TEST_F(ResourceSchedulerTest, testReleaseUnknownCookie) {
  const uint64_t FiveHundredMB = 500000000;
  const void* caller = NULL;

  const void* cookie1 = NULL;
  // Use the address of the scheduler, which is definitely not a valid cookie.
  const void* cookie2 = &MLFQSchedulerWithCookies;

  // Schedule 500MB with cookies.
  ASSERT_NO_THROW(
    cookie1 = MLFQSchedulerWithCookies->scheduleWithCookie(FiveHundredMB,
                                                           caller));

  // Try to release an unknown cookie: the address of the scheduler.
  ASSERT_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie2),
                       AssertionFailedException);

  // Verify that releasing the 500MB cookie does not throw an exception.
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie1));
}

TEST_F(ResourceSchedulerTest, testBlockAtFullCapacity) {
  const uint64_t TwoHundredMB = 200000000;
  const void* caller = NULL;

  const void* cookie1 = NULL;
  const void* cookie2 = NULL;
  const void* cookie3 = NULL;
  const void* cookie4 = NULL;
  const void* cookie5 = NULL;
  const void* cookie6 = NULL;

  // Scheduling 5 200MB buffers should not block
  ASSERT_NO_THROW(
    cookie1 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller));
  ASSERT_NO_THROW(
    cookie2 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller));
  ASSERT_NO_THROW(
    cookie3 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller));
  ASSERT_NO_THROW(
    cookie4 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller));
  ASSERT_NO_THROW(
    cookie5 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller));

  // Scheduling a 6th buffer should block, which will fail an assertion
  // because test mode is on.
  ASSERT_THROW(
    cookie6 = MLFQSchedulerWithCookies->scheduleWithCookie(TwoHundredMB,
                                                           caller),
    AssertionFailedException);

  // Return the 6 buffers
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie1));
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie2));
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie3));
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie4));
  ASSERT_NO_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie5));

  // Returning the 6th should abort because it was never scheduled to begin
  // with.
  ASSERT_THROW(MLFQSchedulerWithCookies->releaseWithCookie(cookie6),
               AssertionFailedException);
}
