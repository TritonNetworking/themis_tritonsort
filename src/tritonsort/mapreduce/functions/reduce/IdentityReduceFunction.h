#ifndef MAPRED_IDENTITY_REDUCE_FUNCTION_H
#define MAPRED_IDENTITY_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class IdentityReduceFunction : public ReduceFunction {
public:
  IdentityReduceFunction() {};
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // MAPRED_IDENTITY_REDUCE_FUNCTION_H
