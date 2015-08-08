#include "KVPairBufferTest.h"
#include "TestMemoryBackedKVPair.h"

#include "mapreduce/common/buffers/KVPairBuffer.h"

void KVPairBufferTest::testMultipleCompleteBuffers() {
  TestMemoryBackedKVPair p1(20, 80);
  TestMemoryBackedKVPair p2(70, 30);
  TestMemoryBackedKVPair p3(65, 35);

  uint64_t totalSize =
    p1.getWriteSize() + p2.getWriteSize() + p3.getWriteSize();

  KVPairBuffer buffer(*memoryAllocator, callerID, totalSize, 12800);
  buffer.addKVPair(p1);
  buffer.addKVPair(p2);
  buffer.addKVPair(p3);

  CPPUNIT_ASSERT_EQUAL(totalSize, buffer.getCurrentSize());
}

void KVPairBufferTest::testSingleCompleteBuffer() {
  TestMemoryBackedKVPair p1(20, 80);

  KVPairBuffer buffer(*memoryAllocator, callerID, p1.getWriteSize(), 12800);
  buffer.addKVPair(p1);

  CPPUNIT_ASSERT_EQUAL(p1.getWriteSize(), buffer.getCurrentSize());
}

void KVPairBufferTest::testEmpty() {
  KVPairBuffer buffer(*memoryAllocator, callerID, 275, 12800);

  CPPUNIT_ASSERT_EQUAL((uint64_t) 0, buffer.getCurrentSize());
}

void KVPairBufferTest::testSetupAndCommitAppendKVPair() {
  uint64_t bufferLength = 10000;
  uint32_t keyLength = 100;
  uint32_t maxValueLength = 700;
  uint32_t valueLength = 300;
  KVPairBuffer buffer(*memoryAllocator, callerID, bufferLength, 12800);

  uint8_t* keyPtr = NULL;
  uint8_t* valPtr = NULL;

  buffer.setupAppendKVPair(keyLength, maxValueLength, keyPtr, valPtr);

  for (uint64_t i = 0; i < keyLength; i++) {
    keyPtr[i] = i;
  }

  for (uint64_t i = 0; i < valueLength; i++) {
    valPtr[i] = i;
  }

  buffer.commitAppendKVPair(keyPtr, valPtr, valueLength);

  buffer.resetIterator();

  KeyValuePair resultKVPair;
  CPPUNIT_ASSERT(buffer.getNextKVPair(resultKVPair));

  const uint8_t* resultKeyPtr = resultKVPair.getKey();
  const uint8_t* resultValPtr = resultKVPair.getValue();
  CPPUNIT_ASSERT_EQUAL(0, memcmp(keyPtr, resultKeyPtr, keyLength));
  CPPUNIT_ASSERT_EQUAL(keyLength, resultKVPair.getKeyLength());
  CPPUNIT_ASSERT_EQUAL(0, memcmp(valPtr, resultValPtr, valueLength));
  CPPUNIT_ASSERT_EQUAL(valueLength, resultKVPair.getValueLength());
}

void KVPairBufferTest::testBogusSetupAndCommitAppendKVPair() {
  KVPairBuffer buffer(*memoryAllocator, callerID, 1000, 12800);

  uint8_t* keyPtr = NULL;
  uint8_t* valPtr = NULL;

  // Shouldn't be able to set up an append bigger than the buffer
  CPPUNIT_ASSERT_THROW(buffer.setupAppendKVPair(100, 2000, keyPtr, valPtr),
                       AssertionFailedException);

  // If we start normally ...
  buffer.setupAppendKVPair(100, 300, keyPtr, valPtr);

  // ... but then commit more than we say we were going to, it should fail.
  CPPUNIT_ASSERT_THROW(buffer.commitAppendKVPair(keyPtr, valPtr, 600),
                       AssertionFailedException);
}
