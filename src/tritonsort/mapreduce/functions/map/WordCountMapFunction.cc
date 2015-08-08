#include "core/ByteOrder.h"
#include "core/Hash.h"
#include "mapreduce/functions/map/WordCountMapFunction.h"

WordCountMapFunction::WordCountMapFunction()
  : one(hostToBigEndian64(1)) {
  std::list<std::string> commonWords;
  commonWords.push_back("A");
  commonWords.push_back("And");
  commonWords.push_back("&");
  commonWords.push_back("As");
  commonWords.push_back("At");
  commonWords.push_back("Be");
  commonWords.push_back("But");
  commonWords.push_back("By");
  commonWords.push_back("Do");
  commonWords.push_back("For");
  commonWords.push_back("From");
  commonWords.push_back("Have");
  commonWords.push_back("He");
  commonWords.push_back("His");
  commonWords.push_back("I");
  commonWords.push_back("I");
  commonWords.push_back("In");
  commonWords.push_back("It");
  commonWords.push_back("Not");
  commonWords.push_back("Of");
  commonWords.push_back("On");
  commonWords.push_back("That");
  commonWords.push_back("The");
  commonWords.push_back("This");
  commonWords.push_back("Too");
  commonWords.push_back("With");
  commonWords.push_back("You");
  commonWords.push_back("a");
  commonWords.push_back("and");
  commonWords.push_back("as");
  commonWords.push_back("at");
  commonWords.push_back("be");
  commonWords.push_back("but");
  commonWords.push_back("by");
  commonWords.push_back("do");
  commonWords.push_back("for");
  commonWords.push_back("from");
  commonWords.push_back("have");
  commonWords.push_back("he");
  commonWords.push_back("his");
  commonWords.push_back("in");
  commonWords.push_back("it");
  commonWords.push_back("not");
  commonWords.push_back("of");
  commonWords.push_back("on");
  commonWords.push_back("that");
  commonWords.push_back("the");
  commonWords.push_back("this");
  commonWords.push_back("to");
  commonWords.push_back("too");
  commonWords.push_back("with");
  commonWords.push_back("you");

  for (std::list<std::string>::iterator iter = commonWords.begin();
       iter != commonWords.end(); iter++) {
    const std::string& word = *iter;
    uint64_t hash = Hash::hash(
      reinterpret_cast<const uint8_t*>(word.c_str()),
      word.size());

    commonKeys.insert(hash);
    counts[hash] = 0;
    hashToWord[hash] = word;
  }
}

void WordCountMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {
  const uint8_t* sentence = kvPair.getValue();
  uint32_t sentenceLength = kvPair.getValueLength();

  tokenizer.loadSentence(sentence, sentenceLength);

  const uint8_t* word = NULL;
  uint32_t wordLength = 0;

  while (tokenizer.nextWord(word, wordLength)) {
    countWord(word, wordLength, writer);
  }
}

void WordCountMapFunction::teardown(KVPairWriterInterface& writer) {
  for (boost::unordered_map<uint64_t, std::string>::iterator iter =
         hashToWord.begin(); iter != hashToWord.end(); iter++) {
    uint64_t hash = iter->first;
    const std::string& word = iter->second;
    uint32_t wordLength = word.size();

    uint64_t count = hostToBigEndian64(counts[hash]);

    writeWord(
      hash, reinterpret_cast<const uint8_t*>(word.c_str()), wordLength, count,
      writer);
  }
}

void WordCountMapFunction::countWord(
  const uint8_t* word, uint32_t wordLength, KVPairWriterInterface& writer) {

  uint64_t hash = Hash::hash(word, wordLength);

  if (commonKeys.count(hash) == 1) {
    counts[hash]++;
  } else {
    writeWord(hash, word, wordLength, one, writer);
  }
}
