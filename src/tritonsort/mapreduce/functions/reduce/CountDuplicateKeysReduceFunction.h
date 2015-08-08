#ifndef MAPRED_COUNT_DUPLICATE_KEYS_REDUCE_FUNCTION_H
#define MAPRED_COUNT_DUPLICATE_KEYS_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

/**
   CountDuplicateKeysReduceFunction checks for duplicate keys. If only one tupl
   e with a given key exists, no output will be emitted. If, however, there are
   duplicate keys, a tuple will be emitted whose key matches the given key and
   whose value is the number of duplicates.
 */
class CountDuplicateKeysReduceFunction : public ReduceFunction {
public:
  /// Constructor
  CountDuplicateKeysReduceFunction() {}

  /// Destructor
  virtual ~CountDuplicateKeysReduceFunction() {}

  /**
     If there's only one tuple with the given key, don't do anything. If there
     are duplicates, then emit a tuple whose key matches the given key and whose
     value is the number of duplicates.

     \sa ReduceFunction::reduce
   */
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // MAPRED_COUNT_DUPLICATE_KEYS_REDUCE_FUNCTION_H
