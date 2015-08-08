#ifndef MAPRED_ZERO_KEY_MAP_FUNCTION_H
#define MAPRED_ZERO_KEY_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class ZeroKeyMapFunction : public MapFunction {
public:
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
};

#endif // MAPRED_ZERO_KEY_MAP_FUNCTION_H
