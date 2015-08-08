#ifndef MAPRED_PASS_THROUGH_MAP_FUNCTION_H
#define MAPRED_PASS_THROUGH_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class PassThroughMapFunction : public MapFunction {
public:
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
};

#endif // MAPRED_PASS_THROUGH_MAP_FUNCTION_H
