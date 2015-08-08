#include "PhaseZeroKVPairWriteStrategy.h"

uint32_t PhaseZeroKVPairWriteStrategy::getOutputKeyLength(
  uint32_t inputKeyLength) const {

  return inputKeyLength;
}

uint32_t PhaseZeroKVPairWriteStrategy::getOutputValueLength(
  uint32_t inputValueLength) const {

  return sizeof(uint64_t);
}

void PhaseZeroKVPairWriteStrategy::writeKey(
  const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey) const {

  memcpy(outputKey, inputKey, inputKeyLength);
}

void PhaseZeroKVPairWriteStrategy::writeValue(
  const uint8_t* inputValue, uint32_t inputValueLength,
  uint32_t inputKeyLength, uint8_t* outputValue) const {

  uint64_t newValue = KeyValuePair::tupleSize(inputKeyLength, inputValueLength);

  memcpy(outputValue, reinterpret_cast<uint8_t*>(&newValue), sizeof(newValue));
}
