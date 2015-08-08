#ifndef MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H
#define MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "mapreduce/common/StreamInfo.h"

class FilenameToStreamIDMapTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( FilenameToStreamIDMapTest );
  CPPUNIT_TEST( testNormalAccess );
  CPPUNIT_TEST( testInvalidFilenameThrowsException );
  CPPUNIT_TEST( testInvalidStreamIDThrowsException );
  CPPUNIT_TEST( testDuplicateFilenameThrowsException );
  CPPUNIT_TEST_SUITE_END();

public:
  void testNormalAccess();
  void testInvalidFilenameThrowsException();
  void testInvalidStreamIDThrowsException();
  void testDuplicateFilenameThrowsException();

private:
  void checkStreamInfo(
    const StreamInfo& streamInfo, const std::string& filename,
    uint64_t streamID, const std::set<uint64_t>& referenceJobIDs);
};

#endif // MAPRED_FILENAME_TO_STREAM_ID_MAP_TEST_H
