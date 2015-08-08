#ifndef THEMIS_MEMORY_UTILS_TEST_H
#define THEMIS_MEMORY_UTILS_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <pthread.h>

class MemoryUtilsTest : public CppUnit::TestFixture {
public:
  CPPUNIT_TEST_SUITE( MemoryUtilsTest );
  CPPUNIT_TEST( testNormalAllocation );
  CPPUNIT_TEST( testReallyHugeAllocationFailsAppropriately );
  CPPUNIT_TEST_SUITE_END();

  void testNormalAllocation();
  void testReallyHugeAllocationFailsAppropriately();
};

#endif // THEMIS_MEMORY_UTILS_TEST_H
