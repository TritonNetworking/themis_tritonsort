#ifndef THEMIS_WORD_TOKENIZER_H
#define THEMIS_WORD_TOKENIZER_H

#include <stdint.h>
#include <vector>

class WordTokenizer {
public:
  WordTokenizer();

  void loadSentence(const uint8_t* sentence, uint32_t sentenceLength);

  bool nextWord(const uint8_t*& word, uint32_t& wordLength);

private:
  std::vector<uint8_t> stopCharacters;

  const uint8_t* sentence;
  uint32_t sentenceLength;
  uint32_t wordStartIndex;
  uint32_t sentenceIndex;
};


#endif // THEMIS_WORD_TOKENIZER_H
