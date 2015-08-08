#ifndef MAPRED_GEN_RANDOM_NETWORK_MAP_FUNCTION_H
#define MAPRED_GEN_RANDOM_NETWORK_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class GenRandomNetworkMapFunction : public MapFunction {
public:
  GenRandomNetworkMapFunction(uint64_t _numVertices, double _edgeProbability,
                              uint64_t _maxNeighbors,
                              uint64_t _mypeerid, uint64_t _numPeers);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t numVertices; // of the graph to be generated
  const double edgeProbability; // between any two vertices
  const uint64_t maxNeighbors; // to cap the size of adjacency list

  const uint64_t mypeerid;
  const uint64_t numPeers;

};

#endif // MAPRED_GEN_RANDOM_NETWORK_MAP_FUNCTION_H
