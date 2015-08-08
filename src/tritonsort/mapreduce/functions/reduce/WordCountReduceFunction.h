#ifndef THEMIS_WORD_COUNT_REDUCE_FUNCTION_H
#define THEMIS_WORD_COUNT_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class WordCountReduceFunction : public ReduceFunction {
public:
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // THEMIS_WORD_COUNT_REDUCE_FUNCTION_H
