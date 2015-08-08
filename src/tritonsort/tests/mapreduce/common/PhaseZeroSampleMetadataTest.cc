#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "tests/mapreduce/common/PhaseZeroSampleMetadataTest.h"

#include <sstream>

void PhaseZeroSampleMetadataTest::testReadAndWriteFromKVPair() {
  PhaseZeroSampleMetadata inputMetadata(0xbadf00d, 42, 16, 85, 12, 12);

  KeyValuePair kvPair;
  inputMetadata.write(kvPair);

  PhaseZeroSampleMetadata outputMetadata(kvPair);

  CPPUNIT_ASSERT(inputMetadata.equals(outputMetadata));
}

void PhaseZeroSampleMetadataTest::testMerge() {
  PhaseZeroSampleMetadata firstMetadata(0xdeadbeef, 42, 16, 85, 12, 24);
  PhaseZeroSampleMetadata secondMetadata(0xdeadbeef, 96, 4, 23, 8, 8);

  firstMetadata.merge(secondMetadata);
  // firstMetadata should contain merged results
  CPPUNIT_ASSERT_EQUAL((uint64_t) 0xdeadbeef, firstMetadata.getJobID());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 138, firstMetadata.getTuplesIn());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 20, firstMetadata.getBytesIn());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 108, firstMetadata.getTuplesOut());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 20, firstMetadata.getBytesOut());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 32, firstMetadata.getBytesMapped());

  // secondMetadata should be unmodified
  CPPUNIT_ASSERT_EQUAL((uint64_t) 0xdeadbeef, secondMetadata.getJobID());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 96, secondMetadata.getTuplesIn());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 4, secondMetadata.getBytesIn());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 23, secondMetadata.getTuplesOut());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 8, secondMetadata.getBytesOut());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 8, secondMetadata.getBytesMapped());
}
