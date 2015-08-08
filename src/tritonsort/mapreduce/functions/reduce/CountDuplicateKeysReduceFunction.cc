#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/reduce/CountDuplicateKeysReduceFunction.h"

void CountDuplicateKeysReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  // Count the number of tuples with this key.
  KeyValuePair kvPair;
  uint64_t numTuples = 0;
  while (iterator.next(kvPair)) {
    numTuples++;
  }

  // We expect to only ever see 1 tuple with the given key.
  if (numTuples > 1) {
    // However if there are duplicates then we should emit a tuple with the
    // key, whose value is the number of duplicates.
    KeyValuePair outputTuple;
    outputTuple.setKey(key, keyLength);
    outputTuple.setValue(
      reinterpret_cast<uint8_t*>(&numTuples), sizeof(numTuples));

    writer.write(outputTuple);
  }
}
