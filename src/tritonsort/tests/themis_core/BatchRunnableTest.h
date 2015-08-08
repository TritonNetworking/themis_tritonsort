#ifndef TRITONSORT_BATCH_RUNNABLE_TEST_H
#define TRITONSORT_BATCH_RUNNABLE_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class BatchRunnableTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( BatchRunnableTest );
  CPPUNIT_TEST( testBasic );
  CPPUNIT_TEST_SUITE_END();

public:
  void testBasic();
};

#endif // TRITONSORT_BATCH_RUNNABLE_TEST_H
