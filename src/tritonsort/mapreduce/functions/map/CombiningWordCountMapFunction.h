#ifndef MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_H
#define MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_H

#include <string.h>

#include "mapreduce/common/AggregatingHashCounter.h"
#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/functions/map/WordCountMapFunction.h"

class CombiningWordCountMapFunction : public WordCountMapFunction {
public:
  CombiningWordCountMapFunction(uint64_t maxCombinerEntries);
  void teardown(KVPairWriterInterface& writer);

protected:
  void countWord(
    const uint8_t* word, uint32_t wordLength, KVPairWriterInterface& writer);
private:
  const uint64_t maxCombinerEntries;

  AggregatingHashCounter<uint64_t>* counts;
};

#endif // MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_H
