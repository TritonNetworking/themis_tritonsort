#include <boost/bind.hpp>

#include "common/SimpleMemoryAllocator.h"
#include "core/Hash.h"
#include "mapreduce/common/ReservoirSamplingKVPairWriter.h"
#include "mapreduce/common/HashedPhaseZeroKVPairWriteStrategy.h"
#include "tests/mapreduce/common/KVPairWriterParentWorker.h"
#include "tests/mapreduce/common/ReservoirSamplingKVPairWriterTest.h"

TEST_F(ReservoirSamplingKVPairWriterTest,
       testSetupAndCommitWriteBeforeSampling) {
  SimpleMemoryAllocator memoryAllocator;
  KVPairWriterParentWorker parent(memoryAllocator, 10000);

  KVPairWriteStrategyInterface* writeStrategy =
    new HashedPhaseZeroKVPairWriteStrategy();

  ReservoirSamplingKVPairWriter writer(
    10000000, NULL,
    boost::bind(
      &KVPairWriterParentWorker::emitBufferFromWriter, &parent, _1, _2),
    boost::bind(
      &KVPairWriterParentWorker::getBufferForWriter, &parent, _1),
    boost::bind(
      &KVPairWriterParentWorker::putBufferFromWriter, &parent, _1),
    boost::bind(
      &KVPairWriterParentWorker::logSample, &parent, _1),
    boost::bind(
      &KVPairWriterParentWorker::logWriteStats, &parent, _1, _2, _3, _4),
    writeStrategy);

  uint32_t keyLength = 10;
  uint8_t key[keyLength];

  for (uint32_t i = 0; i < keyLength; i++) {
    key[i] = i;
  }
  uint32_t maxValueSize = 1000;
  uint32_t actualValueSize = 300;

  uint8_t* valuePtr = writer.setupWrite(key, keyLength, maxValueSize);

  // Write 300 bytes of data into the 1000 byte region given to us by
  // setupWrite, and read it all just to make sure we can

  for (uint32_t i = 0; i < actualValueSize; i++) {
    valuePtr[i] = i;
  }

  // Now commit the write and flush the buffer so we can look at it
  writer.commitWrite(actualValueSize);
  writer.flushBuffers();

  // Make sure that the tuple actually got written as expected
  const std::list<KVPairBuffer*>& emittedBuffers = parent.getEmittedBuffers();

  EXPECT_EQ(static_cast<size_t>(1), emittedBuffers.size());

  KVPairBuffer* buffer = emittedBuffers.front();

  KeyValuePair kvPair;
  EXPECT_EQ(true, buffer->getNextKVPair(kvPair));
  EXPECT_TRUE(buffer != NULL);


  uint64_t expectedKey = Hash::hash(key, keyLength);
  uint64_t expectedValue = KeyValuePair::tupleSize(keyLength, actualValueSize);

  EXPECT_EQ(expectedKey, *(reinterpret_cast<const uint64_t*>(kvPair.getKey())));

  EXPECT_EQ(expectedValue,
            *(reinterpret_cast<const uint64_t*>(kvPair.getValue())));
  delete buffer;
}
