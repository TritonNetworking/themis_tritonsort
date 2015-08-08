#ifndef MAPRED_GEN_RANDOM_RATIO_DATA_MAP_FUNCTION_H
#define MAPRED_GEN_RANDOM_RATIO_DATA_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class GenRandomRatioDataMapFunction : public MapFunction {
public:
  GenRandomRatioDataMapFunction(uint64_t _dataSize, uint64_t _valueDimension,
                                uint64_t _mypeerid, uint64_t _numPeers);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t dataSize;
  const uint64_t valueDimension;

  const uint64_t mypeerid;
  const uint64_t numPeers;

};

#endif // MAPRED_GEN_RANDOM_RATIO_DATA_MAP_FUNCTION_H
