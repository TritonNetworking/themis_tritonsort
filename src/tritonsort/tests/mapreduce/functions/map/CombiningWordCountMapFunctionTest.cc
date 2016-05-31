#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/map/CombiningWordCountMapFunction.h"
#include "tests/mapreduce/functions/map/CombiningWordCountMapFunctionTest.h"
#include "tests/mapreduce/functions/map/CombiningWordCountMapVerifyingWriter.h"

TEST_F(CombiningWordCountMapFunctionTest, testProperCombination) {
  std::string key("spam_file.txt");
  std::string value(
    "Well, there's egg and bacon; egg sausage and bacon; egg and spam; "
    "egg bacon and spam; egg bacon sausage and spam; spam bacon sausage and "
    "spam; spam egg spam spam bacon and spam; spam sausage spam spam bacon "
    "spam tomato and spam");

  KeyValuePair kvPair;
  kvPair.setKey(reinterpret_cast<const uint8_t*>(key.c_str()), key.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(value.c_str()), value.size());

  CombiningWordCountMapVerifyingWriter writer;

  CombiningWordCountMapFunction mapFunction(20);
  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  std::map<std::string, uint64_t> expectedCountsMap;

  expectedCountsMap["Well"] = 1;
  expectedCountsMap["there's"] = 1;
  expectedCountsMap["egg"] = 6;
  expectedCountsMap["and"] = 8;
  expectedCountsMap["bacon"] = 7;
  expectedCountsMap["sausage"] = 4;
  expectedCountsMap["spam"] = 14;
  expectedCountsMap["tomato"] = 1;

  for (std::map<std::string, uint64_t>::iterator
         iter = expectedCountsMap.begin(); iter != expectedCountsMap.end();
       iter++) {
    EXPECT_EQ(iter->second, writer.counts[iter->first].front()) << iter->first;
  }
}

TEST_F(CombiningWordCountMapFunctionTest, testTooManyKeysToCache) {
  std::string key("spam_file.txt");
  std::string value(
    "spam spam spam spam spam spam spam spam LOVELY SPAM spam WONDERFUL SPAM "
    "spam spam spam LOVELY SPAM spam WONDERFUL SPAM spammmmm spammmmm spammmmm "
    "SPAMMMM! bacon eggs sausage and SPAMMMM!");

  KeyValuePair kvPair;
  kvPair.setKey(reinterpret_cast<const uint8_t*>(key.c_str()), key.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(value.c_str()), value.size());

  CombiningWordCountMapVerifyingWriter writer;

  CombiningWordCountMapFunction mapFunction(4);
  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  std::map<std::string, uint64_t> singleEntryCounts;
  singleEntryCounts["spam"] = 13;
  singleEntryCounts["spammmmm"] = 3;
  singleEntryCounts["SPAM"] = 4;
  singleEntryCounts["LOVELY"] = 2;
  singleEntryCounts["WONDERFUL"] = 2;
  singleEntryCounts["bacon"] = 1;
  singleEntryCounts["eggs"] = 1;
  singleEntryCounts["sausage"] = 1;
  singleEntryCounts["and"] = 1;

  for (std::map<std::string, uint64_t>::iterator iter =
         singleEntryCounts.begin(); iter != singleEntryCounts.end(); iter++) {
  EXPECT_EQ(static_cast<uint64_t>(1), writer.counts[iter->first].size());
  EXPECT_EQ(iter->second, writer.counts[iter->first].front());
  }

  std::list<uint64_t> expectedSpammmmmCounts;
  expectedSpammmmmCounts.push_back(1);
  expectedSpammmmmCounts.push_back(1);

  assertListsEqual(expectedSpammmmmCounts, writer.counts["SPAMMMM!"]);
}


void CombiningWordCountMapFunctionTest::assertListsEqual(
  std::list<uint64_t>& expected, std::list<uint64_t>& received) {

  EXPECT_EQ(expected.size(), received.size());

  std::list<uint64_t>::iterator expectedIter = expected.begin();
  std::list<uint64_t>::iterator receivedIter = received.begin();

  while (expectedIter != expected.end()) {
    EXPECT_EQ(*expectedIter, *receivedIter);
    expectedIter++;
    receivedIter++;
  }
}
