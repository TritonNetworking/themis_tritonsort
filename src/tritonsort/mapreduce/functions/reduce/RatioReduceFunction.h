#ifndef MAPRED_RATIO_REDUCE_FUNCTION_H
#define MAPRED_RATIO_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class RatioReduceFunction : public ReduceFunction {
public:
  RatioReduceFunction(double _ratio);
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
private:
  const double ratio;
};

#endif // MAPRED_RATIO_REDUCE_FUNCTION_H
