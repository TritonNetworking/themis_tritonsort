#ifndef MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H
#define MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class BytesCountMapFunctionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( BytesCountMapFunctionTest );
  CPPUNIT_TEST( testProperCountStored );
  CPPUNIT_TEST_SUITE_END();

public:
  BytesCountMapFunctionTest();
  virtual ~BytesCountMapFunctionTest();

  void testProperCountStored();

private:
  static const uint32_t keyLength = 10;
  static const uint32_t valueLength = 90;
  uint8_t* keyBytes;
  uint8_t* valueBytes;
};

#endif // MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H
