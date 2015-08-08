#include <arpa/inet.h>
#include <string.h>
#include <string>

#include "InvertedIndexMapFunction.h"

InvertedIndexMapFunction::InvertedIndexMapFunction(
  uint64_t _numValueBytesPerWord)
  : numValueBytesPerWord(_numValueBytesPerWord) {
}

void InvertedIndexMapFunction::map(KeyValuePair& kvPair,
                                KVPairWriterInterface& writer) {
  WordCountMap wordCounts;

  const uint8_t *wordPtr = kvPair.getValue();
  uint32_t remainingLen = kvPair.getValueLength();

  //split the value into several words
  while (remainingLen > 0) {
    uint32_t wordLen = (remainingLen > numValueBytesPerWord) ?
                           numValueBytesPerWord : remainingLen;

    std::string word ((const char*) wordPtr, (size_t) wordLen);
    ++wordCounts[word];

    remainingLen -= wordLen;
    wordPtr = &wordPtr[wordLen];
  }

  uint32_t valueLength = kvPair.getKeyLength() + sizeof(WordCount);

  for (WordCountMapIter it = wordCounts.begin(); it != wordCounts.end(); it++) {
    const std::string& word = it->first;
    WordCount count = it->second;

    const uint8_t* wordCStr = (const uint8_t*) (word.c_str());
    uint32_t wordLength = word.size();

    uint8_t* outputValue = writer.setupWrite(wordCStr, wordLength,
                                             valueLength);
    memcpy(outputValue, kvPair.getKey(), kvPair.getKeyLength());

    count = htonl(count);
    memcpy(outputValue + kvPair.getKeyLength(), &count, sizeof(count));

    writer.commitWrite(valueLength);
  }
}
