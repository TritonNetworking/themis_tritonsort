#include <boost/filesystem.hpp>
#include <sstream>

#include "core/Params.h"
#include "mapreduce/common/boundary/DiskBackedBoundaryKeyList.h"
#include "mapreduce/common/boundary/PartitionBoundaries.h"
#include "tests/mapreduce/common/DiskBackedBoundaryKeyListTest.h"

extern const char* TEST_WRITE_ROOT;

DiskBackedBoundaryKeyList* DiskBackedBoundaryKeyListTest::newTestKeyList(
  uint64_t numPartitions) {
  boost::filesystem::path testFilePath(
    boost::filesystem::path(TEST_WRITE_ROOT) / "test.boundaries");

  uint64_t jobID = 42;

  std::ostringstream oss;
  oss << "DISK_BACKED_BOUNDARY_LIST." << jobID;

  if (fileExists(testFilePath.c_str())) {
    unlink(testFilePath.c_str());
  }

  Params params;
  params.add<std::string>(oss.str(), testFilePath.string());

  DiskBackedBoundaryKeyList* boundaryKeyList =
    DiskBackedBoundaryKeyList::newBoundaryKeyList(params, jobID, numPartitions);

  for (uint8_t i = 0; i < numPartitions; i++) {
    uint32_t keyLength = i + 1;
    uint8_t* key = new uint8_t[keyLength];
    memset(key, i + 1, keyLength);

    boundaryKeyList->addBoundaryKey(key, keyLength);

    delete[] key;
  }

  return boundaryKeyList;
}

void DiskBackedBoundaryKeyListTest::validateTestKeyList(
  DiskBackedBoundaryKeyList* boundaryKeyList, uint64_t numPartitions) {
  for (uint8_t i = 0; i < numPartitions; i++) {
    PartitionBoundaries* boundaries = boundaryKeyList->getPartitionBoundaries(
      i);

    std::pair<const uint8_t*, uint32_t> lowerBoundKey =
      boundaries->getLowerBoundKey();
    std::pair<const uint8_t*, uint32_t> upperBoundKey =
      boundaries->getUpperBoundKey();

    EXPECT_TRUE(lowerBoundKey.first != NULL);

    for (uint32_t lowerKeyIndex = 0; lowerKeyIndex < lowerBoundKey.second;
         lowerKeyIndex++) {
      EXPECT_EQ(
        static_cast<uint8_t>(i + 1), lowerBoundKey.first[lowerKeyIndex]);
    }

    if (upperBoundKey.first != NULL) {
      for (uint32_t upperKeyIndex = 0; upperKeyIndex < upperBoundKey.second;
           upperKeyIndex++) {
        EXPECT_EQ(
          static_cast<uint8_t>(i + 2), upperBoundKey.first[upperKeyIndex]);
      }
    } else {
      EXPECT_EQ(i, static_cast<uint8_t>(numPartitions - 1));
    }
  }
}

TEST_F(DiskBackedBoundaryKeyListTest, testLoadBoundaryKeys) {
  DiskBackedBoundaryKeyList* boundaryKeyList = newTestKeyList(5);
  validateTestKeyList(boundaryKeyList, 5);
  delete boundaryKeyList;
}

TEST_F(DiskBackedBoundaryKeyListTest, testPersistAcrossReinstantiation) {
  // Create a test boundary list
  DiskBackedBoundaryKeyList* boundaryKeyList = newTestKeyList(5);

  // Now delete it, so that it closes its file
  delete boundaryKeyList;

  // Now re-intantiate it and make sure everything still makes sense
  boost::filesystem::path testFilePath(
    boost::filesystem::path(TEST_WRITE_ROOT) / "test.boundaries");

  Params params;
  params.add<std::string>(
    "DISK_BACKED_BOUNDARY_LIST.22", testFilePath.string());

  DiskBackedBoundaryKeyList* loadedBoundaryList =
    DiskBackedBoundaryKeyList::loadForJob(params, 22);

  validateTestKeyList(loadedBoundaryList, 5);

  delete loadedBoundaryList;
}

TEST_F(DiskBackedBoundaryKeyListTest, testPartitionBoundaryRange) {
  DiskBackedBoundaryKeyList* boundaryKeyList = newTestKeyList(5);

  PartitionBoundaries* boundaries = boundaryKeyList->getPartitionBoundaries(
    1, 3);

  std::pair<const uint8_t*, uint32_t> lowerBoundKey =
    boundaries->getLowerBoundKey();
  std::pair<const uint8_t*, uint32_t> upperBoundKey =
    boundaries->getUpperBoundKey();

  EXPECT_EQ(static_cast<uint32_t>(2), lowerBoundKey.second);
  EXPECT_EQ(static_cast<uint8_t>(2), lowerBoundKey.first[0]);
  EXPECT_EQ(static_cast<uint8_t>(2), lowerBoundKey.first[1]);

  EXPECT_EQ(static_cast<uint32_t>(5), upperBoundKey.second);
  EXPECT_EQ(static_cast<uint8_t>(5), upperBoundKey.first[0]);
  EXPECT_EQ(static_cast<uint8_t>(5), upperBoundKey.first[1]);
  EXPECT_EQ(static_cast<uint8_t>(5), upperBoundKey.first[2]);
  EXPECT_EQ(static_cast<uint8_t>(5), upperBoundKey.first[3]);
  EXPECT_EQ(static_cast<uint8_t>(5), upperBoundKey.first[4]);

  ASSERT_THROW(
    boundaryKeyList->getPartitionBoundaries(1, 5), AssertionFailedException);
  ASSERT_THROW(
    boundaryKeyList->getPartitionBoundaries(3, 2), AssertionFailedException);

  delete boundaries;

  boundaries = boundaryKeyList->getPartitionBoundaries(2, 4);

  EXPECT_EQ(
    static_cast<const uint8_t*>(NULL), boundaries->getUpperBoundKey().first);

  delete boundaries;
}
