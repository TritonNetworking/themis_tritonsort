#include <arpa/inet.h>

#include "PageRankReduceFunction.h"
#include "mapreduce/common/KeyValuePair.h"

void PageRankReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  uint64_t *vertexStruct = NULL;
  uint64_t numNeighbors = 0;

  double pageRankValue = 0.0;

  uint64_t *vertexID;
  vertexID = (uint64_t  *)key;

  uint64_t numPRvalues = 0;

  while (iterator.next(kvPair)) {
    if(kvPair.getValueLength() == sizeof(double)){
      const double *val;
      val = reinterpret_cast<const double *>(kvPair.getValue());

      pageRankValue += *val;
      numPRvalues++;
    } else {
      uint64_t *list = (uint64_t *)kvPair.getValue();
      numNeighbors = list[0];

      ABORT_IF(vertexStruct != NULL,
            "Already received Vertex Structure for vertex %016lX\n", *vertexID);
      vertexStruct = list;
    }
  }

  KeyValuePair outputKVPair;
  outputKVPair.setKey(key, keyLength);

  if (vertexStruct == NULL) {
    // We found pages that link to this page, but no structural tuple for this
    // page. In this case, create a new structural tuple for this page
    // containing its Page Rank and no outgoing edges.
    uint8_t value[16]; // sizeof(uint64_t) + sizeof(double)
    uint8_t* valuePtr = value; // Avoid gcc aliasing warning.
    // This vertex has 0 neighbors in the directed graph.
    *(reinterpret_cast<uint64_t*>(valuePtr)) = 0;
    *(reinterpret_cast<double*>(valuePtr + sizeof(uint64_t))) = pageRankValue;

    outputKVPair.setValue(value, 16);
    writer.write(outputKVPair);
  } else {
    // We found the structural tuple, so write that as the value with the
    // updated Page Rank.
    memcpy(&vertexStruct[numNeighbors + 1], &pageRankValue, sizeof(double));

    outputKVPair.setValue(
      reinterpret_cast<uint8_t *>(vertexStruct),
      (numNeighbors + 2) * sizeof(uint64_t));
    writer.write(outputKVPair);
  }
}
