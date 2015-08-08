#ifndef MAPRED_DEFAULT_KV_PAIR_WRITE_STRATEGY_H
#define MAPRED_DEFAULT_KV_PAIR_WRITE_STRATEGY_H

#include "KVPairWriteStrategyInterface.h"

/**
   DefaultKVPairWriteStrategy writes the record the caller gave it without
   alteration.
 */
class DefaultKVPairWriteStrategy : public KVPairWriteStrategyInterface {
public:
  bool altersKey() const {
    return false;
  }

  bool altersValue() const {
    return false;
  }

  uint32_t getOutputKeyLength(uint32_t inputKeyLength) const;
  uint32_t getOutputValueLength(uint32_t inputValueLength) const;

  void writeKey(
    const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey) const;

  void writeValue(
    const uint8_t* inputValue, uint32_t inputValueLength,
    uint32_t inputKeyLength, uint8_t* outputValue) const;

};

#endif // MAPRED_DEFAULT_KV_PAIR_WRITE_STRATEGY_H
