#include <arpa/inet.h>

#include "InvertedIndexReduceFunction.h"
#include "mapreduce/common/KeyValuePair.h"

void InvertedIndexReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength,
  KVPairIterator& iterator, KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  uint64_t numTuples = 0;

  while (iterator.next(kvPair)) {
    numTuples++;
  }

  iterator.reset();

  uint8_t *outputVal = NULL;
  uint8_t *outputValPtr = NULL;

  uint32_t inputValueLength = 0;

  uint64_t i = 0;

  while (iterator.next(kvPair)) {
    if (i == 0) { //first kvPair
      inputValueLength = kvPair.getValueLength();
      outputVal = writer.setupWrite(
        key, keyLength, numTuples * inputValueLength);
      outputValPtr = outputVal;
    }

    ABORT_IF(inputValueLength != kvPair.getValueLength(),
             "Expected all outputs from map to have same value length");

    memcpy(outputValPtr, kvPair.getValue(), inputValueLength);
    outputValPtr += inputValueLength;

    i++;
  }

  writer.commitWrite(numTuples * inputValueLength);
}
