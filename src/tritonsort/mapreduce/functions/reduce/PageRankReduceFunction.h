#ifndef MAPRED_PAGE_RANK_REDUCE_FUNCTION_H
#define MAPRED_PAGE_RANK_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class PageRankReduceFunction : public ReduceFunction {
public:
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // MAPRED_PAGE_RANK_REDUCE_FUNCTION_H
