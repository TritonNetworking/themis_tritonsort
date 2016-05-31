#include "SampleMetadataKVPairBufferTest.h"
#include "common/SimpleMemoryAllocator.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "mapreduce/common/buffers/SampleMetadataKVPairBuffer.h"

TEST_F(SampleMetadataKVPairBufferTest, testReadAndWriteMetadata) {
  uint64_t keyLength = 92;
  uint64_t valueLength = 900;
  uint64_t numTuples = 10;

  uint64_t tupleSize = KeyValuePair::tupleSize(keyLength, valueLength);

  SampleMetadataKVPairBuffer buffer(
    *memoryAllocator, callerID, numTuples * tupleSize, 0);

  uint8_t* referenceKey = new uint8_t[keyLength];
  uint8_t* referenceValue = new uint8_t[valueLength];

  for (uint64_t keyIndex = 0; keyIndex < keyLength; keyIndex++) {
    referenceKey[keyIndex] = keyIndex;
  }

  for (uint64_t valIndex = 0; valIndex < valueLength; valIndex++) {
    referenceValue[valIndex] = valIndex;
  }

  for (uint64_t tupleNumber = 0; tupleNumber < numTuples; tupleNumber++) {
    uint8_t* keyPtr = NULL;
    uint8_t* valPtr = NULL;

    buffer.setupAppendKVPair(keyLength, valueLength, keyPtr, valPtr);

    memcpy(keyPtr, referenceKey, keyLength);
    memcpy(valPtr, referenceValue, valueLength);

    buffer.commitAppendKVPair(keyPtr, valPtr, valueLength);
  }

  EXPECT_EQ(buffer.getCapacity(), buffer.getCurrentSize());
  EXPECT_EQ(numTuples * tupleSize, buffer.getCapacity());
  EXPECT_EQ(numTuples, buffer.getNumTuples());
  EXPECT_EQ(numTuples * KeyValuePair::tupleSize(keyLength, valueLength),
            buffer.getCurrentSize());

  PhaseZeroSampleMetadata metadata(42, 17, 140, 55, 56, 90210);
  buffer.setSampleMetadata(metadata);

  PhaseZeroSampleMetadata* outputMetadata = buffer.getSampleMetadata();

  EXPECT_TRUE(metadata.equals(*outputMetadata));

  delete outputMetadata;

  buffer.resetIterator();

  KeyValuePair kvPair;
  while (buffer.getNextKVPair(kvPair)) {
    const uint8_t* keyPtr = kvPair.getKey();
    const uint8_t* valPtr = kvPair.getValue();

    EXPECT_EQ(0, memcmp(keyPtr, referenceKey, keyLength));
    EXPECT_EQ(0, memcmp(valPtr, referenceValue, valueLength));
  }

  delete[] referenceKey;
  delete[] referenceValue;
}

TEST_F(SampleMetadataKVPairBufferTest, testPrepareForSending) {
  uint64_t numTuples = 70;
  uint64_t keyLength = 10;
  uint64_t valueLength = 0;

  uint64_t tupleSize = KeyValuePair::tupleSize(keyLength, valueLength);

  SampleMetadataKVPairBuffer buffer(
    *memoryAllocator, callerID, numTuples * tupleSize, 0);

  uint8_t* referenceKey = new uint8_t[keyLength];

  for (uint64_t keyIndex = 0; keyIndex < keyLength; keyIndex++) {
    referenceKey[keyIndex] = keyIndex;
  }

  for (uint64_t tupleNumber = 0; tupleNumber < numTuples; tupleNumber++) {
    uint8_t* keyPtr = NULL;
    uint8_t* valPtr = NULL;

    buffer.setupAppendKVPair(keyLength, valueLength, keyPtr, valPtr);


    memcpy(keyPtr, referenceKey, keyLength);

    buffer.commitAppendKVPair(keyPtr, valPtr, valueLength);
  }

  PhaseZeroSampleMetadata metadata(65, 14, 30, 188, 188, 777);
  buffer.setSampleMetadata(metadata);

  EXPECT_EQ(tupleSize * numTuples, buffer.getCurrentSize());
  EXPECT_EQ(buffer.getCurrentSize(), buffer.getCapacity());
  EXPECT_EQ(numTuples, buffer.getNumTuples());

  buffer.prepareBufferForSending();

  EXPECT_EQ(
    (uint64_t) tupleSize * numTuples + PhaseZeroSampleMetadata::tupleSize(),
    buffer.getCurrentSize());
  EXPECT_EQ(buffer.getCurrentSize(), buffer.getCapacity());
  EXPECT_EQ(numTuples + 1, buffer.getNumTuples());
  EXPECT_EQ(numTuples * tupleSize + PhaseZeroSampleMetadata::tupleSize(),
            buffer.getCurrentSize());

  delete[] referenceKey;
}
