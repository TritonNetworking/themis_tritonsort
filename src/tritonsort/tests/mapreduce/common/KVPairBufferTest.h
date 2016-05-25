#ifndef MAPRED_KV_PAIR_BUFFER_TEST_H
#define MAPRED_KV_PAIR_BUFFER_TEST_H

#include <stdint.h>

#include "config.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/common/MemoryAllocatingTestFixture.h"

class MemoryAllocatorInterface;

class KVPairBufferTest : public MemoryAllocatingTestFixture {
  CPPUNIT_TEST_SUITE( KVPairBufferTest );

#ifdef TRITONSORT_ASSERTS
  CPPUNIT_TEST( testBogusSetupAndCommitAppendKVPair );
#endif //TRITONSORT_ASSERTS

  CPPUNIT_TEST( testMultipleCompleteBuffers );
  CPPUNIT_TEST( testSingleCompleteBuffer );
  CPPUNIT_TEST( testEmpty );
  CPPUNIT_TEST( testSetupAndCommitAppendKVPair );

  CPPUNIT_TEST_SUITE_END();

public:
  void testMultipleCompleteBuffers();
  void testSingleCompleteBuffer();
  void testEmpty();
  void testSetupAndCommitAppendKVPair();
  void testBogusSetupAndCommitAppendKVPair();
};

#endif // MAPRED_KV_PAIR_BUFFER_TEST_H
