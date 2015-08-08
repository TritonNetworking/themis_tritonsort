#ifndef _TRITONSORT_THREADSAFEQUEUETEST_H
#define _TRITONSORT_THREADSAFEQUEUETEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class ThreadSafeQueueTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ThreadSafeQueueTest );
  CPPUNIT_TEST( testPushPop );
  CPPUNIT_TEST( testBlockingPop );
  CPPUNIT_TEST( testEmpty );
  CPPUNIT_TEST_SUITE_END();
public:
  void setUp();
  void tearDown();
  void testPushPop();
  void testBlockingPop();
  void testEmpty();
};
#endif //_TRITONSORT_THREADSAFEQUEUETEST_H
