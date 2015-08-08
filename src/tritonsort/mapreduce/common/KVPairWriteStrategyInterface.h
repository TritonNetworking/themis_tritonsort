#ifndef MAPRED_KV_PAIR_WRITE_STRATEGY_INTERFACE_H
#define MAPRED_KV_PAIR_WRITE_STRATEGY_INTERFACE_H

#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   An interface that describes a tuple writing strategy for KVPairWriter. Used
   so that we can write either both the key and the value or just the key using
   the same Mapper class and map function interface.
 */
class KVPairWriteStrategyInterface {
public:
  /// Destructor
  virtual ~KVPairWriteStrategyInterface() {}

  /// \return true if this write strategy modifies the key that the caller is
  /// writing, false otherwise
  virtual bool altersKey() const = 0;

  /// \return true if this write strategy modifies the value that the caller is
  /// writing, false otherwise
  virtual bool altersValue() const = 0;

  /// Get the length of the key that this writer will write for a given input
  /// key
  /**
     \param inputKeyLength that key's length

     \return the length of the key output by this write strategy in bytes
   */
  virtual uint32_t getOutputKeyLength(uint32_t inputKeyLength) const = 0;

  /// Get the length of the value that this writer will write for a given input
  /// value
  /**
     \param inputValueLength that value's length

     \return the length of the value output by this write strategy in bytes
   */
  virtual uint32_t getOutputValueLength(uint32_t inputValueLength) const = 0;

  /// Write an input key
  /**
     This method is responsible for performing any necessary transformation on
     the key before writing it.

     \param inputKey the key that the caller wants to write

     \param inputKeyLength the length of that key

     \param outputKey the memory into which to write the key. This memory
     region is assumed to be getOutputKeyLength(inputKeyLength) bytes
     long
   */
  virtual void writeKey(
    const uint8_t* inputKey, uint32_t inputKeyLength, uint8_t* outputKey)
    const = 0;

  /// Write an input value
  /**
     This method is responsible for performing any necessary transformation on
     the value before writing it.

     \param inputValue the value that the caller wants to write

     \param inputValueLength the length of that value

     \param inputKeyLength the length of the key that the caller wants to write

     \param outputValue the memory into which to write the value. This memory
     region is assumed to be getOutputValueLength(inputValueLength)
     bytes long
   */
  virtual void writeValue(
    const uint8_t* inputValue, uint32_t inputValueLength,
    uint32_t inputKeyLength, uint8_t* outputValue) const = 0;
};

#endif // MAPRED_KV_PAIR_WRITE_STRATEGY_INTERFACE_H
