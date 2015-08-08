#include <string.h>

#include "core/Hash.h"
#include "mapreduce/functions/map/NGramMapFunction.h"
#include "tests/mapreduce/functions/map/NGramMapFunctionTest.h"
#include "tests/mapreduce/functions/map/StringListVerifyingWriter.h"

void NGramMapFunctionTest::insertExpectedKey(
  std::set<std::string>& expectedKeys, const char* key) {

  uint64_t keyHash = Hash::hash(
    reinterpret_cast<const uint8_t*>(key), strlen(key));

  expectedKeys.insert(
    std::string(reinterpret_cast<char*>(&keyHash), sizeof(uint64_t)));
}

void NGramMapFunctionTest::testMapNGrams() {
  StringListVerifyingWriter writer;

  NGramMapFunction mapFunction(3);

  std::string filename("quick-brown.txt");
  std::string sampleLine("The quick brown fox jumped over the lazy dog");

  KeyValuePair kvPair;

  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(filename.c_str()), filename.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(sampleLine.c_str()), sampleLine.size());

  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  std::set<std::string> expectedKeys;

  insertExpectedKey(expectedKeys, "The quick brown");
  insertExpectedKey(expectedKeys, "quick brown fox");
  insertExpectedKey(expectedKeys, "brown fox jumped");
  insertExpectedKey(expectedKeys, "fox jumped over");
  insertExpectedKey(expectedKeys, "jumped over the");
  insertExpectedKey(expectedKeys, "over the lazy");
  insertExpectedKey(expectedKeys, "the lazy dog");

  std::set<std::string> receivedKeys(writer.keys.begin(), writer.keys.end());

  for (std::set<std::string>::iterator expectedIter = expectedKeys.begin();
       expectedIter != expectedKeys.end(); expectedIter++) {
    CPPUNIT_ASSERT_MESSAGE(
      (*expectedIter).c_str(), receivedKeys.count(*expectedIter) == 1);
  }
}
