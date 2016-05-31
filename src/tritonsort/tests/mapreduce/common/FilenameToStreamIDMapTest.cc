#include "FilenameToStreamIDMapTest.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/StreamInfo.h"

void FilenameToStreamIDMapTest::checkStreamInfo(
  const StreamInfo& streamInfo, const std::string& filename, uint64_t streamID,
  const std::set<uint64_t>& referenceJobIDs) {

  EXPECT_EQ(streamID, streamInfo.getStreamID());

  const std::set<uint64_t>& jobIDs = streamInfo.getJobIDs();

  EXPECT_EQ(streamInfo.getFilename(), filename);
  EXPECT_EQ(referenceJobIDs.size(), jobIDs.size());

  for (std::set<uint64_t>::iterator iter = referenceJobIDs.begin();
       iter != referenceJobIDs.end(); iter++) {
    EXPECT_EQ(static_cast<uint64_t>(1), jobIDs.count(*iter));
  }
}

TEST_F(FilenameToStreamIDMapTest, testNormalAccess) {
  FilenameToStreamIDMap map;

  std::set<uint64_t> firstJobSet;
  firstJobSet.insert(0);
  firstJobSet.insert(1);
  firstJobSet.insert(2);
  std::set<uint64_t> secondJobSet;
  secondJobSet.insert(4);

  map.addFilename("foo", firstJobSet);
  map.addFilename("baz", secondJobSet);
  map.addFilename("quxx", firstJobSet);
  map.addFilename("smoo", secondJobSet);

  checkStreamInfo(map.getStreamInfo("foo"), "foo", 0, firstJobSet);
  checkStreamInfo(map.getStreamInfo("baz"), "baz", 1, secondJobSet);
  checkStreamInfo(map.getStreamInfo("quxx"), "quxx", 2, firstJobSet);
  checkStreamInfo(map.getStreamInfo("smoo"), "smoo", 3, secondJobSet);
}

TEST_F(FilenameToStreamIDMapTest, testInvalidFilenameThrowsException) {
  FilenameToStreamIDMap map;

  ASSERT_THROW(map.getStreamInfo("missing"), AssertionFailedException);
}

TEST_F(FilenameToStreamIDMapTest, testInvalidStreamIDThrowsException) {
  FilenameToStreamIDMap map;

  ASSERT_THROW(map.getStreamInfo(37), AssertionFailedException);
}

TEST_F(FilenameToStreamIDMapTest, testDuplicateFilenameThrowsException) {
  FilenameToStreamIDMap map;
  std::set<uint64_t> jobIDs;
  map.addFilename("duplicate", jobIDs);

  ASSERT_THROW(
    map.addFilename("duplicate", jobIDs), AssertionFailedException);

}
