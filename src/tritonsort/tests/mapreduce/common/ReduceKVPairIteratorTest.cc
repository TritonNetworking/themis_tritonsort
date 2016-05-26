#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/reducer/ReduceKVPairIterator.h"
#include "tests/mapreduce/common/ReduceKVPairIteratorTest.h"

void ReduceKVPairIteratorTest::SetUp() {
  dummyMemory = NULL;
  buffer = NULL;

  // Create a memory region from which we can make keys and values
  dummyMemory = new uint8_t[100];

  for (uint64_t i = 0; i < 100; i++) {
    dummyMemory[i] = i;
  }

  uint8_t* bufferMemory = new uint8_t[10000];

  // Doesn't matter how big the buffer is, just that it's large enough to hold
  // a few records
  buffer = new KVPairBuffer(bufferMemory, 10000);
}

void ReduceKVPairIteratorTest::TearDown() {
  if (dummyMemory != NULL) {
    delete[] dummyMemory;
  }

  if (buffer != NULL) {
    delete buffer;
  }
}

void ReduceKVPairIteratorTest::setupBuffer() {
  KeyValuePair kvPair;

  // Create three records with the key `dummyMemory + 50`
  kvPair.setKey(dummyMemory, 50);
  kvPair.setValue(dummyMemory + 50, 30);

  buffer->addKVPair(kvPair);

  kvPair.setValue(dummyMemory + 40, 20);
  buffer->addKVPair(kvPair);

  kvPair.setValue(dummyMemory + 20, 10);
  buffer->addKVPair(kvPair);

  // Create two records with the key `dummyMemory + 10`
  kvPair.setKey(dummyMemory + 10, 50);
  kvPair.setValue(dummyMemory + 20, 2);

  buffer->addKVPair(kvPair);

  kvPair.setValue(dummyMemory + 4, 42);
  buffer->addKVPair(kvPair);
}

TEST_F(ReduceKVPairIteratorTest, testMultipleKeys) {
  setupBuffer();

  ReduceKVPairIterator iterator(*buffer);

  validateCompleteIteration(iterator);
}

void ReduceKVPairIteratorTest::validateCompleteIteration(
  ReduceKVPairIterator& iterator) {
  KeyValuePair kvPair;

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  // We should first be given `dummyMemory + 50`'s key and be able to iterate
  // through the three records with that key
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(memcmp(dummyMemory, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 20, 10);

  EXPECT_TRUE(!iterator.next(kvPair));

  // Now, we should be given `dummyMemory + 10`'s key, and be able to iterate
  // through its two values
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(
      memcmp(dummyMemory + 10, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  EXPECT_TRUE(!iterator.next(kvPair));

  // At this point, there should be no more records left in the buffer through
  // which to iterate
  EXPECT_TRUE(!iterator.startNextKey(key, keyLength));
}

TEST_F(ReduceKVPairIteratorTest, testEndIterationInMiddle) {
  setupBuffer();

  ReduceKVPairIterator iterator(*buffer);

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  KeyValuePair kvPair;

  // Only iterate through the first two records for key `dummyMemory + 50`
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(memcmp(dummyMemory, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  // Despite the fact that we haven't iterated through the third record for the
  // first key, we should advance to the second key and be able to read all its
  // records
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(
      memcmp(dummyMemory + 10, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  EXPECT_TRUE(!iterator.next(kvPair));
}

TEST_F(ReduceKVPairIteratorTest, testResetInMiddle) {
  setupBuffer();

  ReduceKVPairIterator iterator(*buffer);

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  KeyValuePair kvPair;

  // Start iterating as normal ...
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(memcmp(dummyMemory, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  // Not done iterating through the first key's records, but I'm still
  // resetting
  iterator.reset();

  // The iterator should be placed back at the first record for the first key
  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 20, 10);

  EXPECT_TRUE(!iterator.next(kvPair));

  // Now, start iterating over the second key
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(
      memcmp(dummyMemory + 10, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  // And reset again
  iterator.reset();

  // Should be back at the start of the _second_ key's records
  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  EXPECT_TRUE(!iterator.next(kvPair));

  // And we should also be at the end of the buffer when we're done iterating
  // through them
  EXPECT_TRUE(!iterator.startNextKey(key, keyLength));
}

void ReduceKVPairIteratorTest::validateKVPair(
  KeyValuePair& kvPair, const uint8_t* key, uint32_t keyLength,
  const uint8_t* value, uint32_t valueLength) {

  EXPECT_EQ(keyLength, kvPair.getKeyLength());
  EXPECT_TRUE(memcmp(kvPair.getKey(), key, keyLength) == 0);
  EXPECT_EQ(valueLength, kvPair.getValueLength());
  EXPECT_TRUE(memcmp(kvPair.getValue(), value, valueLength) == 0);
}

TEST_F(ReduceKVPairIteratorTest, testReset) {
  setupBuffer();

  ReduceKVPairIterator iterator(*buffer);

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  KeyValuePair kvPair;

  // Iterate all the way through the first key's records
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(memcmp(dummyMemory, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 20, 10);

  EXPECT_TRUE(!iterator.next(kvPair));

  // Resetting should put you back at the beginning of the first key's records
  iterator.reset();

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 20, 10);

  EXPECT_TRUE(!iterator.next(kvPair));

  // Iterate through the next key
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(
      memcmp(dummyMemory + 10, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  EXPECT_TRUE(!iterator.next(kvPair));

  // Resetting should put you back at the beginning of the _second_ key's
  // records
  iterator.reset();

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  EXPECT_TRUE(!iterator.next(kvPair));

}

TEST_F(ReduceKVPairIteratorTest, testCannotIterateIntoNextKey) {
  setupBuffer();

  ReduceKVPairIterator iterator(*buffer);

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  KeyValuePair kvPair;

  // Iterate through the first key's records
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(memcmp(dummyMemory, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 50, 30);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 40, 20);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory, 50, dummyMemory + 20, 10);

  // At this point, I'm at the end of the first key's records. Calling next()
  // should once should return false and calling it again should fail an assert.
  EXPECT_TRUE(!iterator.next(kvPair));
  ASSERT_THROW(iterator.next(kvPair), AssertionFailedException);

  // At this point I should be able to read the second key's records as normal,
  // despite overdoing it on the next() calls
  EXPECT_TRUE(iterator.startNextKey(key, keyLength));

  EXPECT_EQ(static_cast<uint32_t>(50), keyLength);
  EXPECT_TRUE(
      memcmp(dummyMemory + 10, key, std::min<uint64_t>(keyLength, 50)) == 0);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 20, 2);

  EXPECT_TRUE(iterator.next(kvPair));
  validateKVPair(kvPair, dummyMemory + 10, 50, dummyMemory + 4, 42);

  // Similarly, next() multiple times at the end of the buffer should fail
  // asserts.
  EXPECT_TRUE(!iterator.next(kvPair));
  ASSERT_THROW(iterator.next(kvPair), AssertionFailedException);

  EXPECT_TRUE(!iterator.startNextKey(key, keyLength));
}
