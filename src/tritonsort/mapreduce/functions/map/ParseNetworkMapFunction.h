#ifndef MAPRED_PARSE_NETWORK_MAP_FUNCTION_H
#define MAPRED_PARSE_NETWORK_MAP_FUNCTION_H

#include <string>
#include <string.h>
#include "mapreduce/functions/map/MapFunction.h"

class ParseNetworkMapFunction : public MapFunction {
public:
  ParseNetworkMapFunction(uint64_t _numVertices, std::string _fileURL,
                              uint64_t _maxNeighbors,
                              uint64_t _mypeerid, uint64_t _numPeers);

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t numVertices; // of the graph to be generated
  const std::string fileURL;
  const uint64_t maxNeighbors; // to cap the size of adjacency list

  const uint64_t mypeerid;
  const uint64_t numPeers;

};

#endif // MAPRED_PARSE_NETWORK_MAP_FUNCTION_H
