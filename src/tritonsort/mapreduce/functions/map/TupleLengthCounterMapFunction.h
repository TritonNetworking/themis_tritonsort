#ifndef MAPRED_TUPLE_LENGTH_COUNTER_MAP_FUNCTION_H
#define MAPRED_TUPLE_LENGTH_COUNTER_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class TupleLengthCounterMapFunction : public MapFunction {
public:
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  static const uint64_t COUNT;

  static const uint8_t KEY;
  static const uint8_t VALUE;
  static const uint8_t TUPLE;
};

#endif // MAPRED_TUPLE_LENGTH_COUNTER_MAP_FUNCTION_H
