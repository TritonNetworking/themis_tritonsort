#ifndef MAPRED_K_MEANS_MAP_FUNCTION_H
#define MAPRED_K_MEANS_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class KMeansMapFunction : public MapFunction {
public:
  KMeansMapFunction(uint64_t _dimension, uint64_t _k, std::string _kCentersURL);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
  void init(const Params& params);
  void teardown(KVPairWriterInterface& writer);

private:
  const uint64_t dimension;
  const uint64_t k;
  const std::string kCentersURL;

  uint64_t **kCenters;

};

#endif // MAPRED_K_MEANS_MAP_FUNCTION_H
