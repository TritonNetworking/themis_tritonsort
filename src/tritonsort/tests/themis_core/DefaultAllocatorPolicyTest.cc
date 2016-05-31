#include "DefaultAllocatorPolicyTest.h"
#include "core/DefaultAllocatorPolicy.h"

DefaultAllocatorPolicyTest::DefaultAllocatorPolicyTest()
  : singleTracker("single"),
    chainTracker1("chainA"),
    chainTracker2("chainB"),
    chainTracker3("chainC"),
    context100(0, 100, false),
    context200(0, 200, false),
    context300(0, 300, false),
    context1000(0, 1000, false),
    NULL_REQUEST(NULL) {

  // Construct a graph for a single tracker.
  singleTrackerSet.addTracker(&singleTracker);

  // Construct a graph for a chain of 3 trackers.
  chainTracker1.addDownstreamTracker(&chainTracker2);
  chainTracker2.addDownstreamTracker(&chainTracker3);
  chainTrackerSet.addTracker(&chainTracker1);
  chainTrackerSet.addTracker(&chainTracker2);
  chainTrackerSet.addTracker(&chainTracker3);
}

TEST_F(DefaultAllocatorPolicyTest, testSingleAllocation) {
  DefaultAllocatorPolicy* policy = new DefaultAllocatorPolicy(singleTrackerSet);

  // Create a request for 100 bytes of memory.
  MemoryAllocatorPolicy::Request request(context100, 100);

  // Nothing should be schedulable right now, even with 1000 bytes.
  EXPECT_EQ(NULL_REQUEST, policy->nextSchedulableRequest(1000));

  // Try to add the request to the policy.
  ASSERT_NO_THROW(policy->addRequest(request, "single"));

  // After adding, this request should be schedulable with sufficient memory
  EXPECT_EQ(&request, policy->nextSchedulableRequest(1000));

  // Make sure we can schedule this request with 100 bytes of memory available.
  EXPECT_TRUE(policy->canScheduleRequest(100, request));

  // Try to remove the request.
  ASSERT_NO_THROW(policy->removeRequest(request, "single"));

  // Nothing should be schedulable after removing the request.
  EXPECT_EQ(NULL_REQUEST, policy->nextSchedulableRequest(1000));

  delete policy;
}

TEST_F(DefaultAllocatorPolicyTest, testBlockWhenOutOfMemory) {
  DefaultAllocatorPolicy* policy = new DefaultAllocatorPolicy(singleTrackerSet);

  // Create 2 requests for 100 bytes of memory.
 MemoryAllocatorPolicy::Request request1(context100, 100);
 MemoryAllocatorPolicy::Request request2(context100, 100);

  // Add both requests.
  ASSERT_NO_THROW(policy->addRequest(request1, "single"));
  ASSERT_NO_THROW(policy->addRequest(request2, "single"));

  // Given 150 bytes of memory, the first request should be schedulable.
  uint64_t memory = 150;
  EXPECT_EQ(&request1, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request1));

  // Schedule request 1.
  ASSERT_NO_THROW(policy->removeRequest(request1, "single"));
  memory -= request1.size;

  // Should NOT be able to schedule request2 with only 50 bytes left.
  EXPECT_EQ(NULL_REQUEST, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(!policy->canScheduleRequest(memory, request2));

  // Cleanup (remove request2 so destructor doesn't throw)
  ASSERT_NO_THROW(policy->removeRequest(request2, "single"));

  delete policy;
}

TEST_F(DefaultAllocatorPolicyTest, testPriorityQueues) {
  DefaultAllocatorPolicy* policy = new DefaultAllocatorPolicy(chainTrackerSet);

  // Create a bunch of requests of various sizes
  MemoryAllocatorPolicy::Request request100_1(context100, 100);
  MemoryAllocatorPolicy::Request request100_2(context100, 100);
  MemoryAllocatorPolicy::Request request200(context200, 200);
  MemoryAllocatorPolicy::Request request300(context300, 300);
  MemoryAllocatorPolicy::Request request1000(context1000, 1000);

  // Chain has 3 trackers, A, B, C. Give trackers the following requests:
  // A: 200
  // B: 100_2, 300
  // C: 100_1, 1000
  ASSERT_NO_THROW(policy->addRequest(request100_1, "chainC"));
  ASSERT_NO_THROW(policy->addRequest(request100_2, "chainB"));
  ASSERT_NO_THROW(policy->addRequest(request200, "chainA"));
  ASSERT_NO_THROW(policy->addRequest(request300, "chainB"));
  ASSERT_NO_THROW(policy->addRequest(request1000, "chainC"));

  // Make sure we have enough memory to satisfy all requests.
  uint64_t memory = 100 * 2 + 200 + 300 + 1000;


  // The first request to be scheduled should be 100_1.
  EXPECT_EQ(&request100_1, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request100_1));

  // Schedule 100_1.
  ASSERT_NO_THROW(policy->removeRequest(request100_1, "chainC"));
  memory -= request100_1.size;


  // The next request to be scheduled should be 1000.
  EXPECT_EQ(&request1000, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request1000));

  // Schedule 1000.
  ASSERT_NO_THROW(policy->removeRequest(request1000, "chainC"));
  memory -= request1000.size;


  // The next request to be scheduled should be 100_2.
  EXPECT_EQ(&request100_2, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request100_2));

  // Schedule 100_2.
  ASSERT_NO_THROW(policy->removeRequest(request100_2, "chainB"));
  memory -= request100_2.size;


  // The next request to be scheduled should be 300.
  EXPECT_EQ(&request300, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request300));

  // Schedule 300.
  ASSERT_NO_THROW(policy->removeRequest(request300, "chainB"));
  memory -= request300.size;


  // The last request to be scheduled should be 200.
  EXPECT_EQ(&request200, policy->nextSchedulableRequest(memory));
  EXPECT_TRUE(policy->canScheduleRequest(memory, request200));

  // Schedule 200.
  ASSERT_NO_THROW(policy->removeRequest(request200, "chainA"));
  memory -= request200.size;


  // We should be out of memory with nothing left to schedule.
  EXPECT_EQ((uint64_t) 0, memory);
  EXPECT_EQ(NULL_REQUEST, policy->nextSchedulableRequest(
      std::numeric_limits<uint64_t>::max()));

  delete policy;
}
