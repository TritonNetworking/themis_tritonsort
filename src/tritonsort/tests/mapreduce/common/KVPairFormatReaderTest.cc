#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "tests/mapreduce/common/KVPairFormatReaderTest.h"

KVPairFormatReaderTest::KVPairFormatReaderTest()
  : filename("KVPairFormatReaderTest.cc"),
    downstreamTracker("downstream_stage") {
  jobIDs.insert(42);
  jobIDs.insert(777);
  jobIDs.insert(12345);
  filenameToStreamIDMap.addFilename(filename, jobIDs);

  // Set the raw buffer up so byte i has value i
  for (uint8_t i = 0; i < 250; ++i) {
    rawBuffer[i] = i;
  }
}

void KVPairFormatReaderTest::SetUp() {
  // Create a new converter and put it in front of the mock tracker.
  converter = new ByteStreamConverter(
    0, "converter", *memoryAllocator, 0, 0, filenameToStreamIDMap,
    "KVPairFormatReader", params, "dummy_phase");
  converter->addDownstreamTracker(&downstreamTracker);

  // Create input and stream closed buffers. These will be destructed by the
  // converter.
  inputBuffer = new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  inputBuffer->setStreamID(0);
  streamClosedBuffer =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  streamClosedBuffer->setStreamID(0);
}

void KVPairFormatReaderTest::TearDown() {
  downstreamTracker.deleteAllWorkUnits();
  delete converter;
}

TEST_F(KVPairFormatReaderTest, testReadCompleteTuples) {
  // Create an input buffer with 3 complete tuples with key/value lengths:
  // 10, 90
  // 37, 22
  // 77, 0
  appendTuple(inputBuffer, 10, 90);
  appendTuple(inputBuffer, 37, 22);
  appendTuple(inputBuffer, 7, 0);
  uint64_t expectedBufferSize = KeyValuePair::tupleSize(10, 90) +
    KeyValuePair::tupleSize(37, 22) +
    KeyValuePair::tupleSize(7, 0);

  // Sanity check: make sure the input buffer actually contains the tuples.
  uint8_t* rawInputBuffer = const_cast<uint8_t*>(inputBuffer->getRawBuffer());
  verifyBufferContainsTuple(rawInputBuffer, 10, 90);
  rawInputBuffer = KeyValuePair::nextTuple(rawInputBuffer);
  verifyBufferContainsTuple(rawInputBuffer, 37, 22);
  rawInputBuffer = KeyValuePair::nextTuple(rawInputBuffer);
  verifyBufferContainsTuple(rawInputBuffer, 7, 0);

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(10);
  expectedKeySizes.push_back(37);
  expectedKeySizes.push_back(7);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(90);
  expectedValueSizes.push_back(22);
  expectedValueSizes.push_back(0);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

TEST_F(KVPairFormatReaderTest, testInputBufferTerminatesWithPartialHeader) {
  // Create an input buffer with 1 complete tuple with key/value lengths:
  // 0, 50
  appendTuple(inputBuffer, 0, 50);
  uint64_t expectedBufferSize = inputBuffer->getCurrentSize();

  // Create a 3 byte partial header
  uint32_t partialHeader;
  inputBuffer->append(reinterpret_cast<uint8_t*>(&partialHeader), 3);

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(0);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(50);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

TEST_F(KVPairFormatReaderTest, testInputBufferTerminatesAfterHeader) {
  // Create an input buffer with 1 complete tuple with key/value lengths:
  // 11, 22
  appendTuple(inputBuffer, 11, 22);
  uint64_t expectedBufferSize = inputBuffer->getCurrentSize();
  // Create a real header.
  uint32_t keyLength = 29;
  uint32_t valueLength = 3;
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&keyLength), sizeof(keyLength));
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&valueLength), sizeof(valueLength));

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(11);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(22);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

TEST_F(KVPairFormatReaderTest, testInputBufferTerminatesWithPartialTuple) {
  // Create an input buffer with 1 complete tuple with key/value lengths:
  // 12, 124
  appendTuple(inputBuffer, 12, 124);
  uint64_t expectedBufferSize = inputBuffer->getCurrentSize();
  // Create a real header.
  uint32_t keyLength = 40;
  uint32_t valueLength = 82;
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&keyLength), sizeof(keyLength));
  inputBuffer->append(reinterpret_cast<uint8_t*>(&valueLength),
                     sizeof(valueLength));
  // Copy 8 bytes of key and then stop.
  uint64_t key = 345703;
  inputBuffer->append(reinterpret_cast<uint8_t*>(&key), sizeof(key));

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(12);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(124);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

TEST_F(KVPairFormatReaderTest, testHeaderStraddlesInputBuffers) {
  // Create an input buffer with 5 complete tuple with key/value lengths:
  // 100, 91
  appendTuple(inputBuffer, 100, 91);
  appendTuple(inputBuffer, 100, 91);
  appendTuple(inputBuffer, 100, 91);
  appendTuple(inputBuffer, 100, 91);
  appendTuple(inputBuffer, 100, 91);
  uint64_t expectedBufferSize1 = inputBuffer->getCurrentSize();

  // Create a 6th tuple whose header straddles the buffer boundary, since the
  // first input buffer has (191 + 8) * 5 = 995 bytes and has capacity 1000.
  uint32_t keyLength = 70;
  uint32_t valueLength = 151;
  // Append full key length
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&keyLength), sizeof(keyLength));
  // Only 1 byte remaining, so append part of value length.
  inputBuffer->append(reinterpret_cast<uint8_t*>(&valueLength), 1);

  // Create a second input buffer containing the rest of the tuple.
  ByteStreamBuffer* secondInputBuffer =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  secondInputBuffer->setStreamID(0);
  // Copy the rest of the value length.
  secondInputBuffer->append(reinterpret_cast<uint8_t*>(&valueLength) + 1, 3);
  // Copy the tuple.
  secondInputBuffer->append(
    const_cast<uint8_t*>(rawBuffer), keyLength + valueLength);

  // This tuple will overflow into a separate overflow buffer containing just
  // the one tuple.
  uint64_t expectedBufferSize2 =
    KeyValuePair::tupleSize(keyLength, valueLength);

  // Now append a complete tuple.
  appendTuple(secondInputBuffer, 91, 100);
  // Since this tuple is completely contained in a buffer, it will be emitted as
  // the third output buffer.
  uint64_t expectedBufferSize3 = KeyValuePair::tupleSize(91, 100);

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);
  inputs.push_back(secondInputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize1);
  expectedBufferSizes.push_back(expectedBufferSize2);
  expectedBufferSizes.push_back(expectedBufferSize3);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(70);
  expectedKeySizes.push_back(91);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(91);
  expectedValueSizes.push_back(91);
  expectedValueSizes.push_back(91);
  expectedValueSizes.push_back(91);
  expectedValueSizes.push_back(91);
  expectedValueSizes.push_back(151);
  expectedValueSizes.push_back(100);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

TEST_F(KVPairFormatReaderTest, testTupleStraddlesInputBuffers) {
  // Create an input buffer with 5 complete tuple with key/value lengths:
  // 100, 80
  appendTuple(inputBuffer, 100, 80);
  appendTuple(inputBuffer, 100, 80);
  appendTuple(inputBuffer, 100, 80);
  appendTuple(inputBuffer, 100, 80);
  appendTuple(inputBuffer, 100, 80);
  uint64_t expectedBufferSize1 = inputBuffer->getCurrentSize();

  // Create a 6th tuple whose tuple straddles the buffer boundary, since the
  // first input buffer has (180 + 8) * 5 = 940 bytes and has capacity 1000.
  uint32_t keyLength = 80;
  uint32_t valueLength = 100;
  // Append full key and value lengths
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&keyLength), sizeof(keyLength));
  inputBuffer->append(
    reinterpret_cast<uint8_t*>(&valueLength), sizeof(valueLength));

  // Append 1000 - 940 - 8 = 52 bytes of this tuple to this buffer
  inputBuffer->append(const_cast<uint8_t*>(rawBuffer), 52);

  // Create a second input buffer containing the rest of the tuple.
  ByteStreamBuffer* secondInputBuffer =
    new ByteStreamBuffer(*memoryAllocator, callerID, 1000, 0);
  secondInputBuffer->setStreamID(0);
  // Copy the remaining 180 - 52 = 128 bytes of the tuple
  secondInputBuffer->append(const_cast<uint8_t*>(rawBuffer) + 52, 128);

  // This tuple will overflow into a separate overflow buffer containing just
  // the one tuple.
  uint64_t expectedBufferSize2 =
    KeyValuePair::tupleSize(keyLength, valueLength);

  // Now append a complete tuple.
  appendTuple(secondInputBuffer, 100, 100);
  // Since this tuple is completely contained in a buffer, it will be emitted as
  // the third output buffer.
  uint64_t expectedBufferSize3 = KeyValuePair::tupleSize(100, 100);

  std::list<ByteStreamBuffer*> inputs;
  inputs.push_back(inputBuffer);
  inputs.push_back(secondInputBuffer);

  std::list<uint64_t> expectedBufferSizes;
  expectedBufferSizes.push_back(expectedBufferSize1);
  expectedBufferSizes.push_back(expectedBufferSize2);
  expectedBufferSizes.push_back(expectedBufferSize3);

  std::list<uint32_t> expectedKeySizes;
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(100);
  expectedKeySizes.push_back(80);
  expectedKeySizes.push_back(100);

  std::list<uint32_t> expectedValueSizes;
  expectedValueSizes.push_back(80);
  expectedValueSizes.push_back(80);
  expectedValueSizes.push_back(80);
  expectedValueSizes.push_back(80);
  expectedValueSizes.push_back(80);
  expectedValueSizes.push_back(100);
  expectedValueSizes.push_back(100);

  runInputsAndVerifyOutputs(
    inputs, expectedBufferSizes, expectedKeySizes, expectedValueSizes);
}

void KVPairFormatReaderTest::appendTuple(BaseBuffer* buffer, uint32_t keyLength,
                                         uint32_t valueLength) {
  ASSERT(keyLength + valueLength <= 250,
         "Test tuples can only be 250 (plus header) bytes.");

  KeyValuePair kvPair;
  kvPair.setKey(rawBuffer, keyLength);
  kvPair.setValue(rawBuffer + keyLength, valueLength);

  uint8_t* appendPtr = const_cast<uint8_t*>(
    buffer->setupAppend(kvPair.getWriteSize()));
  kvPair.serialize(appendPtr);
  buffer->commitAppend(appendPtr, kvPair.getWriteSize());
}

void KVPairFormatReaderTest::verifyBufferContainsTuple(uint8_t* buffer,
                                                       uint32_t keyLength,
                                                       uint32_t valueLength) {
  ASSERT(keyLength + valueLength <= 250,
         "Test tuples can only be 250 (plus header) bytes.");
  EXPECT_EQ(keyLength, KeyValuePair::keyLength(buffer));
  EXPECT_EQ(valueLength,
    KeyValuePair::valueLength(buffer));

  // Get the start of the tuple data, which is the start of the key.
  uint8_t* tupleStart = KeyValuePair::key(buffer);
  for (uint8_t i = 0; i < keyLength + valueLength; ++i) {
    EXPECT_EQ(i, tupleStart[i]);
  }
}


void KVPairFormatReaderTest::runInputsAndVerifyOutputs(
  std::list<ByteStreamBuffer*>& inputs,
  const std::list<uint64_t>& expectedBufferSizes,
  const std::list<uint32_t>& keySizes, const std::list<uint32_t>& valueSizes) {
  // Convert all input buffers.
  for (std::list<ByteStreamBuffer*>::iterator iter = inputs.begin();
       iter != inputs.end(); iter++) {
    ASSERT_NO_THROW(converter->run(*iter));
  }

  std::queue<Resource*> outputBuffers(downstreamTracker.getWorkQueue());

  // Verify we have the correct number of output buffers
  EXPECT_EQ(expectedBufferSizes.size(), outputBuffers.size());

  // Verify each output buffer and each tuple within.
  std::list<uint32_t>::const_iterator keySizeIter = keySizes.begin();
  std::list<uint32_t>::const_iterator valueSizeIter = valueSizes.begin();
  std::list<uint64_t>::const_iterator sizeIter = expectedBufferSizes.begin();
  while (!outputBuffers.empty()) {
    KVPairBuffer* outputBuffer =
      dynamic_cast<KVPairBuffer*>(outputBuffers.front());
    outputBuffers.pop();

    EXPECT_TRUE(outputBuffer != NULL);

    // Verify buffer size
    EXPECT_EQ(*sizeIter, outputBuffer->getCurrentSize());
    sizeIter++;

    // Verify tuples.
    KeyValuePair kvPair;

    while (outputBuffer->getNextKVPair(kvPair)) {
      EXPECT_EQ(kvPair.getKeyLength(), *keySizeIter);
      EXPECT_EQ(kvPair.getValueLength(), *valueSizeIter);

      keySizeIter++;
      valueSizeIter++;
    }

    // Verify the output buffer has the right source name and job IDs.
    EXPECT_EQ(filename, outputBuffer->getSourceName());
    const std::set<uint64_t>& outputBufferJobIDs = outputBuffer->getJobIDs();
    EXPECT_EQ(jobIDs.size(), outputBufferJobIDs.size());
    for (std::set<uint64_t>::const_iterator iter = jobIDs.begin();
         iter != jobIDs.end(); iter++) {
      EXPECT_TRUE(outputBufferJobIDs.find(*iter) != outputBufferJobIDs.end());
    }
  }

  // Make sure we used up all of the expected tuples.
  EXPECT_TRUE(keySizeIter == keySizes.end());
  EXPECT_TRUE(valueSizeIter == valueSizes.end());

  // Verify that closing the stream deletes the format reader and does not
  // produce any new output buffers.
  ASSERT_NO_THROW(converter->run(streamClosedBuffer));
  ASSERT_NO_THROW(converter->teardown());
  EXPECT_EQ(expectedBufferSizes.size(),
            downstreamTracker.getWorkQueue().size());
}
