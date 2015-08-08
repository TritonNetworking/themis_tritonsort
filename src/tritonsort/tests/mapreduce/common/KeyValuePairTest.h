#ifndef MAPRED_KEY_VALUE_PAIR_TEST_H
#define MAPRED_KEY_VALUE_PAIR_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>

#include "mapreduce/common/KeyValuePair.h"

class KeyValuePairTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( KeyValuePairTest );
  CPPUNIT_TEST( testSerialize );
  CPPUNIT_TEST( testEqualsNoFields );
  CPPUNIT_TEST( testEqualsDifferentKeyLengths );
  CPPUNIT_TEST( testEqualsDifferentValueLengths );
  CPPUNIT_TEST( testEqualsDifferentKeys );
  CPPUNIT_TEST( testEqualsDifferentValues );
  CPPUNIT_TEST( testKeyLength );
  CPPUNIT_TEST( testValueLength );
  CPPUNIT_TEST( testTupleSize );
  CPPUNIT_TEST( testNextTuple );
  CPPUNIT_TEST( testKey );
  CPPUNIT_TEST( testValue );
  CPPUNIT_TEST( testMultipleTuples );
  CPPUNIT_TEST_SUITE_END();

public:
  void setUp();

  void testSerialize();
  void testEqualsNoFields();
  void testEqualsDifferentKeyLengths();
  void testEqualsDifferentValueLengths();
  void testEqualsDifferentKeys();
  void testEqualsDifferentValues();
  void testKeyLength();
  void testValueLength();
  void testTupleSize();
  void testNextTuple();
  void testKey();
  void testValue();
  void testMultipleTuples();

private:
  uint8_t rawBuffer[1000];
  uint8_t firstByte;
  uint8_t* endOfFirstTuple;
  uint8_t* endOfBuffer;

  uint8_t* makeSequentialBlob(uint64_t length);
  bool kvPairsEqual(const KeyValuePair& pair1, const KeyValuePair& pair2);
};

#endif // MAPRED_KEY_VALUE_PAIR_TEST_H
