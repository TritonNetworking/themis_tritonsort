#ifndef THEMIS_NGRAM_MAP_FUNCTION_H
#define THEMIS_NGRAM_MAP_FUNCTION_H

#include <arpa/inet.h>

#include "core/Hash.h"
#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/functions/map/WordTokenizer.h"

class NGramMapFunction : public MapFunction {
public:
  NGramMapFunction(uint64_t nGramSize);
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t nGramSize;
  const uint64_t one;

  WordTokenizer tokenizer;

  inline void emitNGram(
    KVPairWriterInterface& writer, const uint8_t* nGramStartPtr,
    uint32_t nGramLength) {

    uint64_t hash = Hash::hash(nGramStartPtr, nGramLength);

    uint32_t valueSize = sizeof(one) + nGramLength;

    uint8_t* valuePtr = writer.setupWrite(
      reinterpret_cast<uint8_t*>(&hash), sizeof(hash),
      valueSize);

    *(reinterpret_cast<uint64_t*>(valuePtr)) = one;
    memcpy(valuePtr + sizeof(one), nGramStartPtr, nGramLength);

    writer.commitWrite(valueSize);
  }
};

#endif // THEMIS_NGRAM_MAP_FUNCTION_H
