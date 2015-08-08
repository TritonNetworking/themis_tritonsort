#ifndef MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_REDUCE_FUNCTION_H
#define MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class GenPowerLawRandomNetworkReduceFunction : public ReduceFunction {
public:
  GenPowerLawRandomNetworkReduceFunction
                      (uint64_t _numVertices, uint64_t _maxNeighbors);

  void reduce(const uint8_t* key,
              uint64_t keyLength, KVPairIterator& iterator,
              KVPairWriterInterface& writer);


private:
  const uint64_t numVertices; //size of the graph
  const uint64_t maxNeighbors; //size of the graph
};

#endif // MAPRED_GEN_POWER_LAW_RANDOM_NETWORK_REDUCE_FUNCTION_H
