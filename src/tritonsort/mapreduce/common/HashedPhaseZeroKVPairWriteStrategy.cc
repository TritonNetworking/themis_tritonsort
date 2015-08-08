#include "HashedPhaseZeroKVPairWriteStrategy.h"
#include "core/Hash.h"

uint32_t HashedPhaseZeroKVPairWriteStrategy::getOutputKeyLength(
  uint32_t inputKeyLength) const {

  return sizeof(uint64_t);
}

uint32_t HashedPhaseZeroKVPairWriteStrategy::getOutputValueLength(
  uint32_t inputValueLength) const {

  return sizeof(uint64_t);
}

void HashedPhaseZeroKVPairWriteStrategy::writeKey(
  const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey) const {

  uint64_t hash = Hash::hash(inputKey, inputKeyLength);

  memcpy(outputKey, reinterpret_cast<uint8_t*>(&hash), sizeof(hash));
}

void HashedPhaseZeroKVPairWriteStrategy::writeValue(
  const uint8_t* inputValue, uint32_t inputValueLength,
  uint32_t inputKeyLength, uint8_t* outputValue) const {

  uint64_t kvPairSize = KeyValuePair::tupleSize(
    inputKeyLength, inputValueLength);

  memcpy(
    outputValue, reinterpret_cast<uint8_t*>(&kvPairSize), sizeof(kvPairSize));
}
