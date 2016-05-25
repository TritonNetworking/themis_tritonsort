#include <pthread.h>
#include <unistd.h>

#include "ThreadSafeQueueTest.h"
#include "core/ThreadSafeQueue.h"
#include "core/Utils.h"

ThreadSafeQueue<int>* testQueue;

void* pusherThread(void* arg) {
  testQueue->push(42);
  return NULL;
}

void* popperThread(void* arg) {
  int result = testQueue->blockingPop();
  EXPECT_EQ(42, result);
  return NULL;
}

void ThreadSafeQueueTest::SetUp() {
  testQueue = new ThreadSafeQueue<int>();
}

void ThreadSafeQueueTest::TearDown() {
  delete testQueue;
}

TEST_F(ThreadSafeQueueTest, testPushPop) {
  testQueue->push(1);
  testQueue->push(2);
  testQueue->push(16);
  testQueue->push(9);
  testQueue->push(4);

  int testInt = 764;
  EXPECT_TRUE(testQueue->pop(testInt));
  EXPECT_EQ(1, testInt);
  EXPECT_TRUE(testQueue->pop(testInt));
  EXPECT_EQ(2, testInt);
  EXPECT_TRUE(testQueue->pop(testInt));
  EXPECT_EQ(16, testInt);
  EXPECT_TRUE(testQueue->pop(testInt));
  EXPECT_EQ(9, testInt);
  EXPECT_TRUE(testQueue->pop(testInt));
  EXPECT_EQ(4, testInt);
  EXPECT_TRUE(!testQueue->pop(testInt));
  EXPECT_EQ(4, testInt);
}

TEST_F(ThreadSafeQueueTest, testBlockingPop) {
  pthread_t pusherThreadID;
  pthread_t popperThreadID;

  EXPECT_EQ(0, pthread_create(&popperThreadID, NULL, &popperThread, NULL));
  usleep(10000);
  EXPECT_EQ(0, pthread_create(&pusherThreadID, NULL, &pusherThread, NULL));

  EXPECT_EQ(0, pthread_join(pusherThreadID, NULL));
  EXPECT_EQ(0, pthread_join(popperThreadID, NULL));

  EXPECT_TRUE(testQueue->empty());

  EXPECT_EQ(0, pthread_create(&pusherThreadID, NULL, &pusherThread, NULL));
  EXPECT_EQ(0, pthread_create(&popperThreadID, NULL, &popperThread, NULL));

  EXPECT_EQ(0, pthread_join(pusherThreadID, NULL));
  EXPECT_EQ(0, pthread_join(popperThreadID, NULL));
}

TEST_F(ThreadSafeQueueTest, testEmpty) {
  EXPECT_TRUE(testQueue->empty());
  testQueue->push(17);
  EXPECT_TRUE(!testQueue->empty());
  int dummyInt = 0;
  EXPECT_TRUE(testQueue->pop(dummyInt));
  EXPECT_TRUE(testQueue->empty());
}
