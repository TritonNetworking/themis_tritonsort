#ifndef THEMIS_MAPRED_TEXT_LINE_FORMAT_READER_TEST_H
#define THEMIS_MAPRED_TEXT_LINE_FORMAT_READER_TEST_H

#include <list>
#include <set>
#include <vector>

#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "tests/common/MemoryAllocatingTestFixture.h"
#include "tests/themis_core/MockWorkerTracker.h"

class BaseBuffer;
class ByteStreamBuffer;
class ByteStreamConverter;
class KVPairBuffer;

class TextLineFormatReaderTest : public MemoryAllocatingTestFixture {
  CPPUNIT_TEST_SUITE( TextLineFormatReaderTest );
  CPPUNIT_TEST( testLongLines );
  CPPUNIT_TEST( testNewlineAtEnd );
  CPPUNIT_TEST( testWindowsLineFeeds );
  CPPUNIT_TEST( testEmptyLines );
  CPPUNIT_TEST( testLineSpanningBuffers );
  CPPUNIT_TEST( testWindowsLineFeedSpanningBuffers );
  CPPUNIT_TEST( testLineOverflowsBuffer );
  CPPUNIT_TEST_SUITE_END();
public:
  TextLineFormatReaderTest();
  void setUp();
  void tearDown();
  void testLongLines();
  void testWindowsLineFeeds();
  void testNewlineAtEnd();
  void testEmptyLines();
  void testLineSpanningBuffers();
  void testWindowsLineFeedSpanningBuffers();
  void testLineOverflowsBuffer();

private:
  void testBufferContainsLines(
    KVPairBuffer& buffer, std::vector<std::string>& expectedLines);
  void runInputsAndVerifyOutputs(
    std::list<ByteStreamBuffer*>& inputs,
    const std::list<std::vector<std::string>*>& expectedLinesList);

  const std::string filename;
  std::set<uint64_t> jobIDs;

  // Dummy params.
  Params params;

  ByteStreamConverter* converter;
  MockWorkerTracker downstreamTracker;

  FilenameToStreamIDMap filenameToStreamIDMap;

  ByteStreamBuffer* inputBuffer;
  ByteStreamBuffer* streamClosedBuffer;
};

#endif // THEMIS_MAPRED_TEXT_LINE_FORMAT_READER_TEST_H
