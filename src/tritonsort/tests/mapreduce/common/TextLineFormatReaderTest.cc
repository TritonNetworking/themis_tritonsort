#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "tests/mapreduce/common/TextLineFormatReaderTest.h"

TextLineFormatReaderTest::TextLineFormatReaderTest()
  : filename("TextLineFormatReaderTest.cc"),
    downstreamTracker("downstream_stage") {
  jobIDs.insert(260);
  jobIDs.insert(1);
  filenameToStreamIDMap.addFilename(filename, jobIDs);
}

void TextLineFormatReaderTest::SetUp() {
  // Create a new converter and put it in front of the mock tracker.
  converter = new ByteStreamConverter(
    0, "converter", *memoryAllocator, 500, 0, filenameToStreamIDMap,
    "TextLineFormatReader", params, "dummy_phase");
  converter->addDownstreamTracker(&downstreamTracker);

  // Create input and stream closed buffers. These will be destructed by the
  // converter.
  inputBuffer = new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  inputBuffer->setStreamID(0);
  streamClosedBuffer =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  streamClosedBuffer->setStreamID(0);
}

void TextLineFormatReaderTest::TearDown() {
  downstreamTracker.deleteAllWorkUnits();
  delete converter;
}

TEST_F(TextLineFormatReaderTest, testLongLines) {
  std::string longLineBuffer(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer\nThe Slings and Arrows of outrageous Fortune,");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer");
  expectedLines1.push_back("The Slings and Arrows of outrageous Fortune,");

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(longLineBuffer.c_str()),
    longLineBuffer.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testWindowsLineFeeds) {
  std::string longLineBuffer(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer\r\nThe Slings and Arrows of outrageous Fortune,");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer");
  expectedLines1.push_back("The Slings and Arrows of outrageous Fortune,");

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(longLineBuffer.c_str()),
    longLineBuffer.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testNewlineAtEnd) {
  std::string longLineBuffer(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer\r\nThe Slings and Arrows of outrageous Fortune,\n");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "To be, or not to be, that is the question: Whether 'tis Nobler in the "
    "mind to suffer");
  expectedLines1.push_back("The Slings and Arrows of outrageous Fortune,");

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(longLineBuffer.c_str()),
    longLineBuffer.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testEmptyLines) {
  std::string longLineBuffer(
    "Or to take Arms against a Sea of troubles, And by opposing end them:\n\n"
    "to die,\n\nto sleep\n\nNo more\n");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "Or to take Arms against a Sea of troubles, And by opposing end them:");
  expectedLines1.push_back("to die,");
  expectedLines1.push_back("to sleep");
  expectedLines1.push_back("No more");

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(longLineBuffer.c_str()),
    longLineBuffer.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testLineSpanningBuffers) {
  std::string testString1("Devoutly to be wish'd. To die, to sleep;");
  std::string testString2(
    " To sleep: perchance to dream:\nay, there's the rub;");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "Devoutly to be wish'd. To die, to sleep; To sleep: perchance to dream:");
  expectedLines1.push_back("ay, there's the rub;");

  ByteStreamBuffer* inputBuffer2 =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  inputBuffer2->setStreamID(0);

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(testString1.c_str()), testString1.size());
  inputBuffer2->append(
    reinterpret_cast<const uint8_t*>(testString2.c_str()), testString2.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);
  inputs.push_back(inputBuffer2);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testWindowsLineFeedSpanningBuffers) {
  std::string testString1(
    "Devoutly to be wish'd. To die, to sleep; To sleep: perchance to dream:\r");
  std::string testString2(
    "\nay, there's the rub;");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "Devoutly to be wish'd. To die, to sleep; To sleep: perchance to dream:");
  expectedLines1.push_back("ay, there's the rub;");

  ByteStreamBuffer* inputBuffer2 =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  inputBuffer2->setStreamID(0);

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(testString1.c_str()), testString1.size());
  inputBuffer2->append(
    reinterpret_cast<const uint8_t*>(testString2.c_str()), testString2.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);
  inputs.push_back(inputBuffer2);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

TEST_F(TextLineFormatReaderTest, testLineOverflowsBuffer) {
  std::string longLineBuffer(
    "This is the only test in this suite that does not quote Hamlet\n"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789");
  std::vector<std::string> expectedLines1;
  expectedLines1.push_back(
    "This is the only test in this suite that does not quote Hamlet");

  // This line is 500 characters long and by default the converter only produces
  // 500 byte buffers, so this won't fit in the first buffer.
  std::vector<std::string> expectedLines2;
  expectedLines2.push_back(
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789"
    "01234567890123456789012345678901234567890123456789");

  inputBuffer->append(
    reinterpret_cast<const uint8_t*>(longLineBuffer.c_str()),
    longLineBuffer.size());

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<std::vector<std::string>*> expectedLines;
  expectedLines.push_back(&expectedLines1);
  expectedLines.push_back(&expectedLines2);

  runInputsAndVerifyOutputs(inputs, expectedLines);
}

void TextLineFormatReaderTest::testBufferContainsLines(
  KVPairBuffer& buffer, std::vector<std::string>& expectedLines) {

  KeyValuePair kvPair;

  uint32_t filenameSize = filename.size();

  for (std::vector<std::string>::iterator iter = expectedLines.begin();
       iter != expectedLines.end(); iter++) {
    EXPECT_EQ(true, buffer.getNextKVPair(kvPair));
    if (filenameSize > 0) {
      EXPECT_EQ(filenameSize, kvPair.getKeyLength());
      EXPECT_EQ(0, strncmp(
          filename.c_str(), reinterpret_cast<const char*>(kvPair.getKey()),
          filenameSize));
    }
    EXPECT_EQ(static_cast<uint32_t>(iter->size()), kvPair.getValueLength());
    EXPECT_EQ(0, memcmp(kvPair.getValue(), iter->c_str(), iter->size()));
  }
  EXPECT_TRUE(!buffer.getNextKVPair(kvPair));
}

void TextLineFormatReaderTest::runInputsAndVerifyOutputs(
  std::list<ByteStreamBuffer*>& inputs,
  const std::list<std::vector<std::string>*>& expectedLinesList) {
  // Convert all input buffers.
  for (std::list<ByteStreamBuffer*>::iterator iter = inputs.begin();
       iter != inputs.end(); iter++) {
    ASSERT_NO_THROW(converter->run(*iter));
  }

  // Verify that closing the stream deletes the format reader. and does not
  ASSERT_NO_THROW(converter->run(streamClosedBuffer));
  ASSERT_NO_THROW(converter->teardown());

  std::queue<Resource*> outputBuffers(downstreamTracker.getWorkQueue());

  // Verify we have the correct number of output buffers
  EXPECT_EQ(expectedLinesList.size(), outputBuffers.size());

  // Verify each individual output buffer.
  std::list<std::vector<std::string>*>::const_iterator expectedLinesIter =
    expectedLinesList.begin();
  while (!outputBuffers.empty()) {
    KVPairBuffer* outputBuffer =
      dynamic_cast<KVPairBuffer*>(outputBuffers.front());
    outputBuffers.pop();

    EXPECT_TRUE(outputBuffer != NULL);

    std::vector<std::string>* expectedLines = *expectedLinesIter;
    expectedLinesIter++;
    EXPECT_TRUE(expectedLines != NULL);

    testBufferContainsLines(*outputBuffer, *expectedLines);

    // Verify the output buffer has the right source name and job IDs.
    EXPECT_EQ(filename, outputBuffer->getSourceName());
    const std::set<uint64_t>& outputBufferJobIDs = outputBuffer->getJobIDs();
    EXPECT_EQ(jobIDs.size(), outputBufferJobIDs.size());
    for (std::set<uint64_t>::const_iterator iter = jobIDs.begin();
         iter != jobIDs.end(); iter++) {
      EXPECT_TRUE(outputBufferJobIDs.find(*iter) != outputBufferJobIDs.end());
    }
  }
}
