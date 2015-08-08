#ifndef MAPRED_GEN_RANDOM_K_MEANS_DATA_MAP_FUNCTION_H
#define MAPRED_GEN_RANDOM_K_MEANS_DATA_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class GenRandomKMeansDataMapFunction : public MapFunction {
public:
  GenRandomKMeansDataMapFunction(uint64_t _numPoints, uint64_t _dimension,
                           uint64_t _maxPointCoord,
                           uint64_t _k, std::string _kCentersURL,
                           uint64_t _mypeerid, uint64_t _numPeers);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t numPoints;
  const uint64_t dimension;
  const uint64_t maxPointCoord;
  const uint64_t k;
  const std::string kCentersURL; // directory on NFS to find kmeans

  const uint64_t mypeerid;
  const uint64_t numPeers;

};

#endif // MAPRED_GEN_RANDOM_K_MEANS_DATA_MAP_FUNCTION_H
