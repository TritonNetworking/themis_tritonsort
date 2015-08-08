#ifndef MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_MAP_FUNCTION_H
#define MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class GenPowerLawRandomNetworkMapFunction : public MapFunction {
public:
  GenPowerLawRandomNetworkMapFunction(uint64_t _numVertices,
                                      uint64_t _maxVertexDegree,
                                      uint64_t _minVertexDegree,
                                      uint64_t _mypeerid,
                                      uint64_t _numPeers);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t numVertices; // of the graph to be generated
  const uint64_t maxVertexDegree;
  const uint64_t minVertexDegree;

  const uint64_t mypeerid;
  const uint64_t numPeers;

};

#endif // MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_MAP_FUNCTION_H
