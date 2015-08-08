#include <boost/filesystem.hpp>
#include <iostream>

#include "mapreduce/functions/map/WEXTextExtractorMapFunction.h"
#include "tests/mapreduce/functions/map/StringListVerifyingWriter.h"
#include "tests/mapreduce/functions/map/WEXTextExtractorMapFunctionTest.h"

extern const char* TEST_BINARY_ROOT;

void WEXTextExtractorMapFunctionTest::testTextExtraction() {
  WEXTextExtractorMapFunction mapFunction;

  StringListVerifyingWriter writer;

  KeyValuePair kvPair;

  boost::filesystem::path sampleDataPath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "mapreduce" / "functions" /
    "map" / "sample_wex_data.tsv");

  std::string sampleDataFile(sampleDataPath.string());

  std::ifstream fileStream(sampleDataFile.c_str());

  // Map the first line
  std::string firstLine;
  std::getline(fileStream, firstLine);

  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(sampleDataFile.c_str()),
    sampleDataFile.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(firstLine.c_str()), firstLine.size());

  mapFunction.map(kvPair, writer);

  // We expect that the map function will return (pagename, article contents)
  // tuples
  std::list<std::string> pageNames;
  std::list<std::string> articleContents;

  // Check first line
  pageNames.push_back("孫悟空");
  articleContents.push_back("孫悟空 or 孙悟空 may refer to:\\n\\n");

  checkWriter(writer, pageNames, articleContents);

  pageNames.clear();
  articleContents.clear();

  // Map the second line
  std::string secondLine;
  std::getline(fileStream, secondLine);

  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(sampleDataFile.c_str()),
    sampleDataFile.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(secondLine.c_str()), secondLine.size());

  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  // Check second line
  pageNames.push_back("Morphological computation");
  articleContents.push_back("Morphological computation may refer to:\\n\\n");

  checkWriter(writer, pageNames, articleContents);
}

void WEXTextExtractorMapFunctionTest::testMissingArticleContents() {
  // Occasionally the 5th column will be missing in the WEX database. The mapper
  // will simply ignore rows with this property.
  std::string rowMissingArticleContents =
    "Some PageID\tSome Pagename\tSome Datestring\tSome XML";
  std::string filename = "Some Filename";

  WEXTextExtractorMapFunction mapFunction;
  StringListVerifyingWriter writer;

  // Map the row.
  KeyValuePair kvPair;
  kvPair.setKey(
    reinterpret_cast<const uint8_t*>(filename.c_str()), filename.size());
  kvPair.setValue(
    reinterpret_cast<const uint8_t*>(rowMissingArticleContents.c_str()),
    rowMissingArticleContents.size());

  mapFunction.map(kvPair, writer);
  mapFunction.teardown(writer);

  // We expect no outputs from the map function.
  std::list<std::string> pageNames;
  std::list<std::string> articleContents;

  checkWriter(writer, pageNames, articleContents);
}

void WEXTextExtractorMapFunctionTest::checkWriter(
  StringListVerifyingWriter& writer, const std::list<std::string>& keys,
  const std::list<std::string>& values) {

  // Check that the lists have the same sizes.
  CPPUNIT_ASSERT_EQUAL(keys.size(), writer.keys.size());
  CPPUNIT_ASSERT_EQUAL(values.size(), writer.values.size());

  // Check keys
  for (std::list<std::string>::const_iterator expectedIter = keys.begin(),
         actualIter = writer.keys.begin(); expectedIter != keys.end();
       ++expectedIter, ++actualIter) {
    CPPUNIT_ASSERT_EQUAL(*expectedIter, *actualIter);
  }

  // Check values
  for (std::list<std::string>::const_iterator expectedIter = values.begin(),
         actualIter = writer.values.begin(); expectedIter != values.end();
       ++expectedIter, ++actualIter) {
    CPPUNIT_ASSERT_EQUAL(*expectedIter, *actualIter);
  }

  writer.keys.clear();
  writer.values.clear();
}
