#include "mapreduce/functions/map/CombiningWordCountMapFunction.h"
#include "core/Hash.h"

CombiningWordCountMapFunction::CombiningWordCountMapFunction(
  uint64_t _maxCombinerEntries)
  : maxCombinerEntries(_maxCombinerEntries),
    counts(NULL) {
}

void CombiningWordCountMapFunction::teardown(KVPairWriterInterface& writer) {
  if (counts != NULL) {
    counts->flush();
    delete counts;
  }

  WordCountMapFunction::teardown(writer);
}

void CombiningWordCountMapFunction::countWord(
  const uint8_t* word, uint32_t wordLength, KVPairWriterInterface& writer) {

  if (counts == NULL) {
    counts = new AggregatingHashCounter<uint64_t>(maxCombinerEntries, writer);
  }

  counts->add(word, wordLength, 1);
}
