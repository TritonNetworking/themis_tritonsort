#ifndef MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_TEST_H
#define MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_TEST_H

#include "tests/common/MemoryAllocatingTestFixture.h"

class SampleMetadataKVPairBufferTest : public MemoryAllocatingTestFixture {
  CPPUNIT_TEST_SUITE( SampleMetadataKVPairBufferTest );
  CPPUNIT_TEST( testReadAndWriteMetadata );
  CPPUNIT_TEST( testPrepareForSending );
  CPPUNIT_TEST_SUITE_END();
public:
  void testReadAndWriteMetadata();
  void testPrepareForSending();
};

#endif // MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_TEST_H
