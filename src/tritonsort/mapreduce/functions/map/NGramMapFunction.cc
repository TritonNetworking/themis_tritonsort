#include "core/ByteOrder.h"
#include "mapreduce/functions/map/NGramMapFunction.h"

NGramMapFunction::NGramMapFunction(uint64_t _nGramSize)
  : nGramSize(_nGramSize),
    one(hostToBigEndian64(1)) {
}

void NGramMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {

  const uint8_t* sentence = kvPair.getValue();
  uint32_t sentenceLength = kvPair.getValueLength();

  tokenizer.loadSentence(sentence, sentenceLength);

  uint64_t nGramStartIndices[nGramSize];
  uint32_t nGramLengths[nGramSize];

  memset(nGramStartIndices, 0, sizeof(uint64_t) * nGramSize);
  memset(nGramLengths, 0, sizeof(uint32_t) * nGramSize);

  uint64_t words = 0;

  const uint8_t* word = NULL;
  uint32_t wordLength = 0;

  while (tokenizer.nextWord(word, wordLength)) {
    nGramStartIndices[words % nGramSize] = word - sentence;
    nGramLengths[words % nGramSize] = wordLength;
    words++;

    // If you have enough words to form an n-gram, emit one
    if (words >= nGramSize) {
      uint64_t startWord = (words - nGramSize) % nGramSize;
      uint64_t endWord = (words - 1) % nGramSize;

      uint64_t nGramStart = nGramStartIndices[startWord];
      uint32_t nGramLength = nGramStartIndices[endWord] + nGramLengths[endWord]
        - nGramStart;

      if (nGramStart + nGramLength == sentenceLength - 1) {
        // Count the last character in the string if you're at the end of
        // the sentence
        nGramLength++;
      }

      emitNGram(writer, sentence + nGramStart, nGramLength);
    }
  }
}
