#ifndef TRITONSORT_RESOURCE_QUEUE_TEST_H
#define TRITONSORT_RESOURCE_QUEUE_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>

class ResourceQueueTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ResourceQueueTest );
  CPPUNIT_TEST( testEmpty );
  CPPUNIT_TEST( testSize );
  CPPUNIT_TEST( testFront );
  CPPUNIT_TEST( testStealFrom );

  CPPUNIT_TEST_SUITE_END();
public:
  void testEmpty();
  void testSize();
  void testFront();
  void testStealFrom();
private:
  void testStealFrom(uint64_t sourceQueueSize, uint64_t destQueueSize,
                     uint64_t numSourceElements, uint64_t numDestElements,
                     uint64_t sourceOffset, uint64_t destOffset,
                     uint64_t numElementsToCopy);
};

#endif // TRITONSORT_RESOURCE_QUEUE_TEST_H
