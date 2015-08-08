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
  CPPUNIT_ASSERT_EQUAL(42, result);
  return NULL;
}

void ThreadSafeQueueTest::setUp() {
  testQueue = new ThreadSafeQueue<int>();
}

void ThreadSafeQueueTest::tearDown() {
  delete testQueue;
}

void ThreadSafeQueueTest::testPushPop() {
  testQueue->push(1);
  testQueue->push(2);
  testQueue->push(16);
  testQueue->push(9);
  testQueue->push(4);

  int testInt = 764;
  CPPUNIT_ASSERT(testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(1, testInt);
  CPPUNIT_ASSERT(testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(2, testInt);
  CPPUNIT_ASSERT(testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(16, testInt);
  CPPUNIT_ASSERT(testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(9, testInt);
  CPPUNIT_ASSERT(testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(4, testInt);
  CPPUNIT_ASSERT(!testQueue->pop(testInt));
  CPPUNIT_ASSERT_EQUAL(4, testInt);
}

void ThreadSafeQueueTest::testBlockingPop() {
  pthread_t pusherThreadID;
  pthread_t popperThreadID;

  CPPUNIT_ASSERT_EQUAL(0, pthread_create(
                         &popperThreadID, NULL, &popperThread, NULL));
  usleep(10000);
  CPPUNIT_ASSERT_EQUAL(0, pthread_create(
                         &pusherThreadID, NULL, &pusherThread, NULL));

  CPPUNIT_ASSERT_EQUAL(0, pthread_join(pusherThreadID, NULL));
  CPPUNIT_ASSERT_EQUAL(0, pthread_join(popperThreadID, NULL));

  CPPUNIT_ASSERT(testQueue->empty());

  CPPUNIT_ASSERT_EQUAL(0, pthread_create(
                         &pusherThreadID, NULL, &pusherThread, NULL));
  CPPUNIT_ASSERT_EQUAL(0, pthread_create(
                         &popperThreadID, NULL, &popperThread, NULL));

  CPPUNIT_ASSERT_EQUAL(0, pthread_join(pusherThreadID, NULL));
  CPPUNIT_ASSERT_EQUAL(0, pthread_join(popperThreadID, NULL));
}

void ThreadSafeQueueTest::testEmpty() {
  CPPUNIT_ASSERT(testQueue->empty());
  testQueue->push(17);
  CPPUNIT_ASSERT(!testQueue->empty());
  int dummyInt = 0;
  CPPUNIT_ASSERT(testQueue->pop(dummyInt));
  CPPUNIT_ASSERT(testQueue->empty());
}
