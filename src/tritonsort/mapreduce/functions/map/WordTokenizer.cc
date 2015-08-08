#include <stdlib.h>

#include "mapreduce/functions/map/WordTokenizer.h"

WordTokenizer::WordTokenizer() {
  stopCharacters.push_back(' ');
  stopCharacters.push_back('"');
  stopCharacters.push_back('.');
  stopCharacters.push_back(',');
  stopCharacters.push_back('/');
  stopCharacters.push_back('/');
  stopCharacters.push_back('(');
  stopCharacters.push_back(')');
  stopCharacters.push_back('?');
  stopCharacters.push_back(';');
}

void WordTokenizer::loadSentence(
  const uint8_t* _sentence, uint32_t _sentenceLength) {

  sentence = _sentence;
  sentenceLength = _sentenceLength;
  wordStartIndex = 0;
  sentenceIndex = 0;
}

bool WordTokenizer::nextWord(const uint8_t*& word, uint32_t& wordLength) {
  word = NULL;
  wordLength = 0;

  while (sentenceIndex < sentenceLength && word == NULL) {
    uint8_t currentCharacter = sentence[sentenceIndex];
    bool stopCharacter = false;

    if (sentenceIndex < sentenceLength - 1 && currentCharacter == '\\' &&
        sentence[sentenceIndex + 1] == 'n') {
      // Break on '\n'
      stopCharacter = true;
    }

    for (std::vector<uint8_t>::iterator iter = stopCharacters.begin();
         !stopCharacter && iter != stopCharacters.end(); iter++) {
      if (currentCharacter == *iter) {
        stopCharacter = true;
      }
    }

    if (stopCharacter) {
      if (wordStartIndex == sentenceIndex) {
        // Ignore multiple contiguous stop characters
        wordStartIndex++;
      } else {
        word = sentence + wordStartIndex;
        wordLength = sentenceIndex - wordStartIndex;
        wordStartIndex = sentenceIndex + 1;
      }
    }
    sentenceIndex++;
  }

  if (word == NULL && wordStartIndex < sentenceLength) {
    // Count a word at the end of the line without spaces behind it
    word = sentence + wordStartIndex;
    wordLength = sentenceLength - wordStartIndex;
    wordStartIndex = sentenceIndex + 1;
  }

  return (word != NULL);
}

