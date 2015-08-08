#include <arpa/inet.h>

#include "PageRankMapFunction.h"

PageRankMapFunction::PageRankMapFunction() {
}

void PageRankMapFunction::map(KeyValuePair& kvPair,
                                KVPairWriterInterface& writer) {

  uint64_t *vertexStruct = (uint64_t *)kvPair.getValue();
  uint64_t numNeighbors = vertexStruct[0];

  double vertexPageRank;
  memcpy(&vertexPageRank, &vertexStruct[numNeighbors + 1], sizeof(double));


  if(numNeighbors == 0) { // passing the PageRank to itself
    KeyValuePair outputKVPair;
    outputKVPair.setKey(kvPair.getKey(), kvPair.getKeyLength());
    outputKVPair.setValue(
      reinterpret_cast<const uint8_t*>(&vertexPageRank),
      sizeof(double));
    writer.write(outputKVPair);
  } else {
    double neighborPageRankContrib = vertexPageRank / numNeighbors;
    for(uint64_t i = 1; i <= numNeighbors; i++){
      uint64_t neighborID = vertexStruct[i];

      KeyValuePair outputKVPair;
      outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&neighborID),
          sizeof(uint64_t));
      outputKVPair.setValue(reinterpret_cast<const uint8_t *>
          (&neighborPageRankContrib),
          sizeof(double));
      writer.write(outputKVPair);

    }
  }

  // also pass the full vertex data structure
  writer.write(kvPair);
}
