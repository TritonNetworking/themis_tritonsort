#include <boost/filesystem.hpp>
#include <iostream>
#include <sstream>

#include "mapreduce/functions/map/WEXLinkExtractorMapFunction.h"
#include "tests/mapreduce/functions/map/WEXLinkExtractorMapFunctionTest.h"

extern const char* TEST_BINARY_ROOT;

void WEXLinkExtractorMapFunctionTest::testLinkExtraction() {
  WEXLinkExtractorMapFunctionVerifyingWriter writer;

  WEXLinkExtractorMapFunction mapFunction;

  KeyValuePair kvPair;

  boost::filesystem::path sampleDataPath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "mapreduce" / "functions" /
    "map" / "sample_wex_data.tsv");

  std::string sampleDataFile(sampleDataPath.string());

  std::ifstream fileStream(sampleDataFile.c_str());

  std::string firstLine;

  std::getline(fileStream, firstLine);

  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(sampleDataFile.c_str()),
    sampleDataFile.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(firstLine.c_str()), firstLine.size());

  mapFunction.map(kvPair, writer);

  std::string firstLineKey("孫悟空");

  std::set<std::string> expected;
  expected.insert("Sun Wukong");
  expected.insert("Son Goku (disambiguation)");
  expected.insert("ja:孫悟空 (曖昧さ回避)");

  checkWriter(writer, firstLineKey, expected);

  std::string secondLine;
  std::getline(fileStream, secondLine);

  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(sampleDataFile.c_str()),
    sampleDataFile.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(secondLine.c_str()), secondLine.size());

  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  std::string secondLineKey("Morphological computation");

  expected.clear();
  expected.insert("Morphological computation (robotics)");
  expected.insert("Computational linguistics");

  checkWriter(writer, secondLineKey, expected);
}

void WEXLinkExtractorMapFunctionTest::checkWriter(
  WEXLinkExtractorMapFunctionVerifyingWriter& writer, const std::string& key,
  const std::set<std::string>& values) {

  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(1), writer.keys.size());
  CPPUNIT_ASSERT_EQUAL(key, *(writer.keys.begin()));

  CPPUNIT_ASSERT_EQUAL(values.size(), writer.internalLinks.size());

  for (std::set<std::string>::iterator iter = values.begin();
       iter != values.end(); iter++) {
    CPPUNIT_ASSERT_EQUAL(
      static_cast<uint64_t>(1), writer.internalLinks.count(*iter));
  }

  writer.keys.clear();
  writer.internalLinks.clear();
}
