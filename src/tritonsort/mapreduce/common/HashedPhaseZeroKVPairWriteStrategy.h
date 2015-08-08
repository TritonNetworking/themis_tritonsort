#ifndef MAPRED_HASHED_PHASE_ZERO_KV_PAIR_WRITE_STRATEGY_H
#define MAPRED_HASHED_PHASE_ZERO_KV_PAIR_WRITE_STRATEGY_H

#include "KVPairWriteStrategyInterface.h"

/**
   This write strategy writes the hash of the written key as the key, and the
   original key/value pair's length as the value
 */
class HashedPhaseZeroKVPairWriteStrategy : public KVPairWriteStrategyInterface {
public:

  bool altersKey() const {
    return true;
  }

  bool altersValue() const {
    return true;
  }

  uint32_t getOutputKeyLength(uint32_t inputKeyLength) const;
  uint32_t getOutputValueLength(uint32_t inputValueLength) const;

  void writeKey(
    const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey) const;

  void writeValue(
    const uint8_t* inputValue, uint32_t inputValueLength,
    uint32_t inputKeyLength, uint8_t* outputValue) const;
};

#endif // MAPRED_HASHED_PHASE_ZERO_KV_PAIR_WRITE_STRATEGY_H
