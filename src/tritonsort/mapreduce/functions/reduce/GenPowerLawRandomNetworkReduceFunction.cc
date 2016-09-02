#include <arpa/inet.h>

#include "GenPowerLawRandomNetworkReduceFunction.h"
#include "mapreduce/common/KeyValuePair.h"

GenPowerLawRandomNetworkReduceFunction::GenPowerLawRandomNetworkReduceFunction
     (uint64_t _numVertices, uint64_t _maxNeighbors)
     : numVertices(_numVertices), maxNeighbors(_maxNeighbors) {
}

void GenPowerLawRandomNetworkReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  uint64_t numNeighbors = 0;

  KeyValuePair kvPair;

  while (iterator.next(kvPair)) {
    numNeighbors++;
  }

  iterator.reset();

  numNeighbors -= 1; //discount one emit with value = 0

  if (numNeighbors > maxNeighbors) { // truncate down
    numNeighbors = maxNeighbors;
  }

  uint64_t *outputValue = new uint64_t[numNeighbors + 2];
  outputValue[0] = numNeighbors;

  double initialPageRank = 1.0 / numVertices;
  memcpy(&outputValue[numNeighbors + 1], &initialPageRank, sizeof(double));

  uint64_t j = 1;

  while (iterator.next(kvPair)) {
    TRITONSORT_ASSERT(kvPair.getValueLength() == sizeof(uint64_t), "Invalid vertex value");
    const uint64_t *pNeighborID = reinterpret_cast<const uint64_t *>(
      kvPair.getValue());

    if(*pNeighborID != 0){
      outputValue[j++] = *pNeighborID;
    }
    if(j > numNeighbors) {
      break;
    }
  }

  KeyValuePair outputKVPair;
  outputKVPair.setKey(key, keyLength);
  outputKVPair.setValue(reinterpret_cast<const uint8_t *>(outputValue),
      (numNeighbors + 2) * sizeof(uint64_t));

  writer.write(outputKVPair);

  delete[] outputValue;
}
