#include "DefaultKVPairWriteStrategy.h"

uint32_t DefaultKVPairWriteStrategy::getOutputKeyLength(
  uint32_t inputKeyLength) const {

  return inputKeyLength;
}

uint32_t DefaultKVPairWriteStrategy::getOutputValueLength(
  uint32_t inputValueLength) const {

  return inputValueLength;
}

void DefaultKVPairWriteStrategy::writeKey(
  const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey) const {

  memcpy(outputKey, inputKey, inputKeyLength);
}

void DefaultKVPairWriteStrategy::writeValue(
  const uint8_t* inputValue, uint32_t inputValueLength, uint32_t inputKeyLength,
  uint8_t* outputValue) const {

  memcpy(outputValue, inputValue, inputValueLength);
}
