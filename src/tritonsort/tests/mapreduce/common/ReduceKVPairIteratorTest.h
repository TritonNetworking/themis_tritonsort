#ifndef THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H
#define THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>
#include <stdint.h>

class ReduceKVPairIterator;
class KVPairBuffer;

class ReduceKVPairIteratorTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ReduceKVPairIteratorTest );
  CPPUNIT_TEST( testMultipleKeys );
  CPPUNIT_TEST( testResetInMiddle );
  CPPUNIT_TEST( testEndIterationInMiddle );
  CPPUNIT_TEST( testReset );
  CPPUNIT_TEST( testCannotIterateIntoNextKey );
  CPPUNIT_TEST_SUITE_END();
public:
  void setUp();
  void tearDown();
  void testEndIterationInMiddle();
  void testResetInMiddle();
  void testMultipleKeys();
  void testReset();
  void testCannotIterateIntoNextKey();

private:
  void validateKVPair(
    KeyValuePair& kvPair, const uint8_t* key, uint32_t keyLength,
    const uint8_t* value, uint32_t valueLength);

  void setupBuffer();
  void validateCompleteIteration(ReduceKVPairIterator& iterator);

  KVPairBuffer* buffer;
  uint8_t* dummyMemory;
};

#endif // THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H
