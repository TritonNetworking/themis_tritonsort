#ifndef MAPRED_BYTES_COUNT_MAP_FUNCTION_H
#define MAPRED_BYTES_COUNT_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class BytesCountMapFunction : public MapFunction {
public:
  BytesCountMapFunction(uint64_t numKeyBytesToCount);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t numKeyBytesToCount;
  const static uint64_t COUNT;
};

#endif // MAPRED_BYTES_COUNT_MAP_FUNCTION_H
