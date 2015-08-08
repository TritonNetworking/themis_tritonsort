#ifndef THEMIS_BYTE_ORDER_TEST_H
#define THEMIS_BYTE_ORDER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class ByteOrderTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ByteOrderTest );
  CPPUNIT_TEST( testHostToNetwork );
  CPPUNIT_TEST( testNetworkToHost );
  CPPUNIT_TEST( testCyclicConversion );
  CPPUNIT_TEST_SUITE_END();
public:
  ByteOrderTest();
  void testHostToNetwork();
  void testNetworkToHost();
  void testCyclicConversion();

  uint64_t deadbeef;

  uint64_t deadbeefBigEndian;
  uint64_t deadbeefLittleEndian;
  uint64_t deadbeefHostOrder;
};

#endif // THEMIS_BYTE_ORDER_TEST_H
