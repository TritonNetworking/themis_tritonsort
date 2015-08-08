#ifndef THEMIS_MEMORY_ALLOCATOR_TESTS_H
#define THEMIS_MEMORY_ALLOCATOR_TESTS_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class MemoryAllocatorTests : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( MemoryAllocatorTests );
  CPPUNIT_TEST( testLargeMemoryAllocationFails );
//  CPPUNIT_TEST( testDeadlockDetection );
  CPPUNIT_TEST_SUITE_END();

public:
  void testLargeMemoryAllocationFails();
  void testDeadlockDetection();
};

#endif // THEMIS_MEMORY_ALLOCATOR_TESTS_H
