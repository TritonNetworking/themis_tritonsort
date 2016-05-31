#ifndef THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H
#define THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H

#include <stdint.h>

#include "gtest/gtest.h"

class ReduceKVPairIterator;
class KVPairBuffer;

class ReduceKVPairIteratorTest : public ::testing::Test {
public:
  void SetUp();
  void TearDown();

protected:
  void validateKVPair(
    KeyValuePair& kvPair, const uint8_t* key, uint32_t keyLength,
    const uint8_t* value, uint32_t valueLength);

  void setupBuffer();
  void validateCompleteIteration(ReduceKVPairIterator& iterator);

  KVPairBuffer* buffer;
  uint8_t* dummyMemory;
};

#endif // THEMIS_REDUCE_KV_PAIR_ITERATOR_TEST_H
