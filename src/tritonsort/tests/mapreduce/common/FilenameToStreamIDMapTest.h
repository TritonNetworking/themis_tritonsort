#ifndef MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H
#define MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H

#include "gtest/gtest.h"

#include "mapreduce/common/StreamInfo.h"

class FilenameToStreamIDMapTest : public ::testing::Test {
protected:
  void checkStreamInfo(
    const StreamInfo& streamInfo, const std::string& filename,
    uint64_t streamID, const std::set<uint64_t>& referenceJobIDs);
};

#endif // MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H
