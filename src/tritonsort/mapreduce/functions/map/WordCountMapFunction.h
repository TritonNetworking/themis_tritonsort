#ifndef MAPRED_WORD_COUNT_MAP_FUNCTION_H
#define MAPRED_WORD_COUNT_MAP_FUNCTION_H

#include <boost/unordered_map.hpp>
#include <set>
#include <string>

#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/functions/map/WordTokenizer.h"

class WordCountMapFunction : public MapFunction {
public:
  WordCountMapFunction();

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
  void teardown(KVPairWriterInterface& writer);

protected:
  virtual void countWord(
    const uint8_t* word, uint32_t wordLength, KVPairWriterInterface& writer);

private:
  inline void writeWord(
    uint64_t hash, const uint8_t* word, uint32_t wordLength, uint64_t count,
    KVPairWriterInterface& writer) {

    uint8_t* valuePtr = writer.setupWrite(
      reinterpret_cast<uint8_t*>(&hash), sizeof(hash),
      sizeof(count) + wordLength);

    *(reinterpret_cast<uint64_t*>(valuePtr)) = count;
    memcpy(valuePtr + sizeof(count), word, wordLength);

    writer.commitWrite(sizeof(count) + wordLength);
  }

  // Count will be initialized to 1, since the count for a word is always 1
  const uint64_t one;

  boost::unordered_map<uint64_t, uint64_t> counts;
  boost::unordered_map<uint64_t, std::string> hashToWord;
  std::set<uint64_t> commonKeys;

  WordTokenizer tokenizer;
};

#endif // MAPRED_WORD_COUNT_MAP_FUNCTION_H
