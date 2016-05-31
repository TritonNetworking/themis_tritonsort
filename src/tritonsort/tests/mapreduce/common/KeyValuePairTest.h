#ifndef MAPRED_KEY_VALUE_PAIR_TEST_H
#define MAPRED_KEY_VALUE_PAIR_TEST_H

#include <stdint.h>
#include "gtest/gtest.h"

#include "mapreduce/common/KeyValuePair.h"

class KeyValuePairTest : public ::testing::Test {
public:
  void SetUp();

protected:
  uint8_t rawBuffer[1000];
  uint8_t firstByte;
  uint8_t* endOfFirstTuple;
  uint8_t* endOfBuffer;

  uint8_t* makeSequentialBlob(uint64_t length);
  bool kvPairsEqual(const KeyValuePair& pair1, const KeyValuePair& pair2);
};

#endif // MAPRED_KEY_VALUE_PAIR_TEST_H
