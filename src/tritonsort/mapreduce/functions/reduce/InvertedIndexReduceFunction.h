#ifndef MAPRED_INVERTED_INDEX_REDUCE_FUNCTION_H
#define MAPRED_INVERTED_INDEX_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class InvertedIndexReduceFunction : public ReduceFunction {
public:
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // MAPRED_INVERTED_INDEX_REDUCE_FUNCTION_H
