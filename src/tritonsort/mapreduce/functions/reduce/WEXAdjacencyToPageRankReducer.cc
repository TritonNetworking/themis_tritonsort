#include "core/Hash.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/reduce/WEXAdjacencyToPageRankReducer.h"

const double WEXAdjacencyToPageRankReducer::INITIAL_PAGERANK = 1.0;

void WEXAdjacencyToPageRankReducer::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  uint64_t keyHash = Hash::hash(key, keyLength);

  KeyValuePair kvPair;

  uint64_t numTuples = 0;

  while (iterator.next(kvPair)) {
    numTuples++;
  }

  iterator.reset();

  // Reserve space for each adjacency in the list (we assume no duplicates
  // because WEX claims to only specify each internal target once)
  uint64_t valueSize = ((numTuples + 1) * sizeof(uint64_t)) + sizeof(double);

  uint8_t* value = writer.setupWrite(
    reinterpret_cast<uint8_t*>(&keyHash), sizeof(uint64_t), valueSize);

  uint32_t valueOffset = 0;

  memcpy(value + valueOffset, &numTuples, sizeof(uint64_t));
  valueOffset += sizeof(uint64_t);

  while (iterator.next(kvPair)) {
    uint64_t adjacencyHash = Hash::hash(
      kvPair.getValue(), kvPair.getValueLength());
    memcpy(value + valueOffset, &adjacencyHash, sizeof(uint64_t));
    valueOffset += sizeof(uint64_t);
  }

  memcpy(value + valueOffset, &INITIAL_PAGERANK, sizeof(double));
  valueOffset += sizeof(double);

  writer.commitWrite(valueOffset);
}
