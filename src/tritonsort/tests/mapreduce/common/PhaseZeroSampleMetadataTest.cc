#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "tests/mapreduce/common/PhaseZeroSampleMetadataTest.h"

#include <sstream>

TEST_F(PhaseZeroSampleMetadataTest, testReadAndWriteFromKVPair) {
  PhaseZeroSampleMetadata inputMetadata(0xbadf00d, 42, 16, 85, 12, 12);

  KeyValuePair kvPair;
  inputMetadata.write(kvPair);

  PhaseZeroSampleMetadata outputMetadata(kvPair);

  EXPECT_TRUE(inputMetadata.equals(outputMetadata));
}

TEST_F(PhaseZeroSampleMetadataTest, testMerge) {
  PhaseZeroSampleMetadata firstMetadata(0xdeadbeef, 42, 16, 85, 12, 24);
  PhaseZeroSampleMetadata secondMetadata(0xdeadbeef, 96, 4, 23, 8, 8);

  firstMetadata.merge(secondMetadata);
  // firstMetadata should contain merged results
  EXPECT_EQ((uint64_t) 0xdeadbeef, firstMetadata.getJobID());
  EXPECT_EQ((uint64_t) 138, firstMetadata.getTuplesIn());
  EXPECT_EQ((uint64_t) 20, firstMetadata.getBytesIn());
  EXPECT_EQ((uint64_t) 108, firstMetadata.getTuplesOut());
  EXPECT_EQ((uint64_t) 20, firstMetadata.getBytesOut());
  EXPECT_EQ((uint64_t) 32, firstMetadata.getBytesMapped());

  // secondMetadata should be unmodified
  EXPECT_EQ((uint64_t) 0xdeadbeef, secondMetadata.getJobID());
  EXPECT_EQ((uint64_t) 96, secondMetadata.getTuplesIn());
  EXPECT_EQ((uint64_t) 4, secondMetadata.getBytesIn());
  EXPECT_EQ((uint64_t) 23, secondMetadata.getTuplesOut());
  EXPECT_EQ((uint64_t) 8, secondMetadata.getBytesOut());
  EXPECT_EQ((uint64_t) 8, secondMetadata.getBytesMapped());
}
