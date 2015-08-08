#ifndef MAPRED_K_MEANS_REDUCE_FUNCTION_H
#define MAPRED_K_MEANS_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class KMeansReduceFunction : public ReduceFunction {
public:
  KMeansReduceFunction(uint64_t _dimension, std::string _kCentersURL);
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
private:
  const uint64_t dimension;
  const std::string kCentersURL;
};

#endif // MAPRED_K_MEANS_REDUCE_FUNCTION_H
