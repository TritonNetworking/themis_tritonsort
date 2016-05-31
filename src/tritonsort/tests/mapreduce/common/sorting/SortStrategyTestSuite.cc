#include "core/Comparison.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/common/sorting/SortStrategyTestSuite.h"

void SortStrategyTestSuite::SetUp() {
  inputBuffer = NULL;
  inputBufferMemory = NULL;
  outputBuffer = NULL;
  outputBufferMemory = NULL;
  scratchMemory = NULL;
}

void SortStrategyTestSuite::TearDown() {
  if (inputBuffer != NULL) {
    delete inputBuffer;
  }

  if (inputBufferMemory != NULL) {
    delete[] inputBufferMemory;
  }

  if (outputBuffer != NULL) {
    delete outputBuffer;
  }

  if (outputBufferMemory != NULL) {
    delete[] outputBufferMemory;
  }
}

void SortStrategyTestSuite::setupUniformRecordSizeBuffer(
  uint32_t numRecords, uint32_t keyLength, uint32_t valueLength) {

  inputBuffer = NULL;
  inputBufferMemory = NULL;

  uint64_t memorySize = KeyValuePair::tupleSize(keyLength, valueLength) *
    numRecords;

  inputBufferMemory = new (themis::memcheck) uint8_t[memorySize];

  // Keys are a single repeated byte, cycling between 100 randomly-generated
  // (but deterministic) values. Values alternate between a single repeated 42,
  // 17, or 63.

  uint64_t numKeyByteValues = 100;
  uint8_t keyByteValues[100] = {
    239, 57, 233, 194, 52, 15, 177, 236, 231, 45, 176, 198, 81, 134, 73, 95,
    135, 178, 162, 241, 93, 109, 44, 132, 86, 32, 109, 37, 183, 217, 174, 235,
    246, 133, 1, 163, 249, 49, 232, 68, 72, 227, 234, 115, 251, 116, 28, 38,
    103, 203, 130, 90, 142, 151, 92, 91, 0, 157, 108, 220, 54, 41, 189, 68, 241,
    184, 95, 13, 190, 112, 35, 221, 140, 26, 134, 65, 76, 136, 79, 48, 111, 57,
    148, 60, 233, 196, 233, 8, 110, 102, 55, 104, 123, 185, 246, 149, 129, 39,
    243, 41 };

  uint64_t numValueByteValues = 3;
  uint8_t valueByteValues[3] = { 42, 17, 63 };

  inputBuffer = new KVPairBuffer(inputBufferMemory, memorySize);

  uint8_t* key = NULL;
  uint8_t* value = NULL;

  for (uint32_t i = 0; i < numRecords; i++) {
    inputBuffer->setupAppendKVPair(keyLength, valueLength, key, value);

    memset(key, keyByteValues[i % numKeyByteValues], keyLength);
    memset(value, valueByteValues[i % numValueByteValues], valueLength);

    inputBuffer->commitAppendKVPair(key, value, valueLength);
  }
}

void SortStrategyTestSuite::setupRandomlySizedRecordsBuffer(
  uint64_t numRecords) {

  uint64_t numKeyByteValues = 100;
  uint8_t keyByteValues[100] = {
    239, 57, 233, 194, 52, 15, 177, 236, 231, 45, 176, 198, 81, 134, 73, 95,
    135, 178, 162, 241, 93, 109, 44, 132, 86, 32, 109, 37, 183, 217, 174, 235,
    246, 133, 1, 163, 249, 49, 232, 68, 72, 227, 234, 115, 251, 116, 28, 38,
    103, 203, 130, 90, 142, 151, 92, 91, 0, 157, 108, 220, 54, 41, 189, 68, 241,
    184, 95, 13, 190, 112, 35, 221, 140, 26, 134, 65, 76, 136, 79, 48, 111, 57,
    148, 60, 233, 196, 233, 8, 110, 102, 55, 104, 123, 185, 246, 149, 129, 39,
    243, 41 };

  uint64_t numValueByteValues = 3;
  uint8_t valueByteValues[3] = { 42, 17, 63 };

  uint64_t numKeyLengths = 15;
  uint64_t keyLengths[15] = {
    61, 14, 43, 7, 46, 54, 44, 72, 27, 62, 81, 85, 11, 78, 73 };

  uint64_t numValueLengths = 7;
  uint64_t valueLengths[7] = { 190, 284, 120, 177, 0, 251, 103 };

  // Going to generate records where key and value are all N copies of a single
  // byte. The length of the key and value, as well as their contents, is
  // determined by round-robining through the arrays above.

  // First, we have to figure out how big to make the buffer
  uint64_t memorySize = 0;

  for (uint64_t i = 0; i < numRecords; i++) {
    memorySize += KeyValuePair::tupleSize(
      keyLengths[i % numKeyLengths], valueLengths[i % numValueLengths]);
  }

  inputBufferMemory = new (themis::memcheck) uint8_t[memorySize];

  inputBuffer = new KVPairBuffer(inputBufferMemory, memorySize);

  uint8_t* key = NULL;
  uint32_t keyLength = 0;
  uint8_t* value = NULL;
  uint32_t valueLength = 0;

  for (uint64_t i = 0; i < numRecords; i++) {
    keyLength = keyLengths[i % numKeyLengths];
    valueLength = valueLengths[i % numValueLengths];

    inputBuffer->setupAppendKVPair(keyLength, valueLength, key, value);
    uint8_t keyByte = keyByteValues[i % numKeyByteValues];
    memset(key, keyByte, keyLength);

    uint8_t valueByte = valueByteValues[i % numValueByteValues];
    memset(value, valueByte, valueLength);

    inputBuffer->commitAppendKVPair(key, value, valueLength);
  }
}

void SortStrategyTestSuite::assertSorted(
  KVPairBuffer* buffer, bool checkSecondaryKeys) {

  KeyValuePair kvPair;

  buffer->resetIterator();

  const uint8_t* prevKey = NULL;
  uint32_t prevKeyLength = 0;
  const uint8_t* prevValue = NULL;
  uint32_t prevValueLength = 0;

  while (buffer->getNextKVPair(kvPair)) {
    if (prevKey != NULL) {
      int comp = compare(
        prevKey, prevKeyLength, kvPair.getKey(), kvPair.getKeyLength());

      EXPECT_TRUE(comp <= 0);

      if (checkSecondaryKeys && comp == 0 &&
          prevValueLength >= sizeof(uint64_t) &&
          kvPair.getValueLength() >= sizeof(uint64_t)) {
        uint64_t prevValueSecondaryKey = *(reinterpret_cast<const uint64_t*>(
          prevValue));
        uint64_t currentValueSecondaryKey = *(reinterpret_cast<const uint64_t*>(
          kvPair.getValue()));

        EXPECT_TRUE(prevValueSecondaryKey <= currentValueSecondaryKey);
      }
    }

    prevKey = kvPair.getKey();
    prevKeyLength = kvPair.getKeyLength();
    prevValue = kvPair.getValue();
    prevValueLength = kvPair.getValueLength();
  }
}

void SortStrategyTestSuite::setupOutputBuffer() {
  uint64_t capacity = inputBuffer->getCapacity();
  outputBufferMemory = new (themis::memcheck) uint8_t[capacity];

  outputBuffer = new KVPairBuffer(outputBufferMemory, capacity);
}

void SortStrategyTestSuite::setupScratchSpace(SortStrategyInterface& strategy) {
  scratchMemory = new (themis::memcheck) uint8_t[
    strategy.getRequiredScratchBufferSize(inputBuffer)];
  strategy.setScratchBuffer(scratchMemory);
}

void SortStrategyTestSuite::testUniformSizeRecords(
  SortStrategyInterface& sortStrategy,
  uint32_t numRecords, uint32_t keyLength, uint32_t valueLength,
  bool secondaryKeys) {

  setupUniformRecordSizeBuffer(numRecords, keyLength, valueLength);
  setupOutputBuffer();
  setupScratchSpace(sortStrategy);

  sortStrategy.sort(inputBuffer, outputBuffer);

  assertSorted(outputBuffer, secondaryKeys);
}

void SortStrategyTestSuite::testVariableSizeRecords(
  SortStrategyInterface& sortStrategy, uint32_t numRecords,
  bool secondaryKeys) {

  setupRandomlySizedRecordsBuffer(numRecords);
  setupOutputBuffer();
  setupScratchSpace(sortStrategy);

  sortStrategy.sort(inputBuffer, outputBuffer);

  assertSorted(outputBuffer, secondaryKeys);

}
