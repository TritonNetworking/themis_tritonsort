#ifndef TRITONSORT_RESOURCE_SCHEDULER_TEST_H
#define TRITONSORT_RESOURCE_SCHEDULER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <pthread.h>

#include "core/FCFSPolicy.h"
#include "core/MLFQPolicy.h"

class ResourceScheduler;

class ResourceSchedulerTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ResourceSchedulerTest );
  CPPUNIT_TEST( testFCFSPolicyOrdering );
  CPPUNIT_TEST( testMLFQPolicyOrdering );
  CPPUNIT_TEST( testScheduleSingleResourceWithoutCookies );
  CPPUNIT_TEST( testScheduleSingleResourceWithCookies );
  CPPUNIT_TEST( testReleaseUnknownCookie );
  CPPUNIT_TEST( testBlockAtFullCapacity );
  CPPUNIT_TEST_SUITE_END();

public:
  void setUp();
  void tearDown();
  void testFCFSPolicyOrdering();
  void testMLFQPolicyOrdering();
  void testScheduleSingleResourceWithoutCookies();
  void testScheduleSingleResourceWithCookies();
  void testReleaseUnknownCookie();
  void testBlockAtFullCapacity();

private:
  FCFSPolicy FCFS;
  MLFQPolicy MLFQ;
  pthread_mutex_t sharedLock;
  ResourceScheduler* FCFSSchedulerWithoutCookies;
  ResourceScheduler* MLFQSchedulerWithCookies;
  SchedulerPolicy::Request* NULL_REQUEST;
};

#endif // TRITONSORT_RESOURCE_SCHEDULER_TEST_H
