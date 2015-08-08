#ifndef MAPRED_RATIO_MAP_FUNCTION_H
#define MAPRED_RATIO_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class RatioMapFunction : public MapFunction {
public:
  RatioMapFunction(double _ratio);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const double ratio;
};

#endif // MAPRED_RATIO_MAP_FUNCTION_H
