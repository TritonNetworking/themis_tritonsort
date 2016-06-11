#include <iostream>
#include <re2/re2.h>

#include "tests/themis_core/StatWriterTest.h"

#include "core/File.h"
#include "core/Params.h"
#include "core/StatWriter.h"
#include "core/Utils.h"

extern const char* TEST_WRITE_ROOT;

using namespace re2;

TEST_F(StatWriterTest, testNormalOperation) {
  Params params;
  params.add<bool>("ENABLE_STAT_WRITER", true);
  params.add<std::string>("LOG_DIR", TEST_WRITE_ROOT);

  StatWriter::init(params);
  StatWriter::setCurrentPhaseName("test_phase_before");

  StatLogger* testLogger = new StatLogger("test_logger");
  uint64_t dummyStatID = testLogger->registerStat("dummy_stat");

  StatWriter::spawn();

  StatWriter::setCurrentPhaseName("test_phase");

  testLogger->add(dummyStatID, 42);
  testLogger->add(dummyStatID, 64);

  delete testLogger;
  testLogger = NULL;

  StatWriter::teardown();

  std::string hostname;
  getHostname(hostname);

  std::string logFilePath(
    std::string(TEST_WRITE_ROOT) + "/" + hostname + "_stats.log");

  File logFile(logFilePath);

  logFile.open(File::READ);
  uint64_t fileSize = logFile.getCurrentSize();

  EXPECT_TRUE(fileSize > 0);

  char* buffer = new char[fileSize];
  memset(buffer, 0, fileSize);

  logFile.read(reinterpret_cast<uint8_t*>(buffer), fileSize);

  // There should be eight lines total, two for the stats themselves and 6 for
  // summary statistics

  std::vector<uint64_t> lineStartIndices;
  lineStartIndices.push_back(0);

  for (uint64_t i = 0; i < fileSize; i++) {
    if (buffer[i] == '\n') {
      if (i + 1 < fileSize) {
        lineStartIndices.push_back(i + 1);
      }
      buffer[i] = 0;
    }
  }

  uint64_t numLines = lineStartIndices.size();

  EXPECT_EQ(static_cast<uint64_t>(8), numLines);

  RE2 collLineRegex(
    "COLL\\s+test_phase\\s+0\\s+test_logger\\s+dummy_stat\\s+[0-9]+\\s+"
    "([0-9]+)");

  uint64_t stat;

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[0]), collLineRegex, &stat));

  EXPECT_EQ(static_cast<uint64_t>(42), stat);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[1]), collLineRegex, &stat));

  EXPECT_EQ(static_cast<uint64_t>(64), stat);

  RE2 statLineRegex("SUMM\\s+test_phase\\s+0\\s+test_logger\\s+dummy_stat"
                         "\\s+(.*?)\\s+([0-9]+)");

  std::string statName;
  uint64_t statValue;

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[2]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("min"), statName);
  EXPECT_EQ(static_cast<uint64_t>(42), statValue);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[3]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("max"), statName);
  EXPECT_EQ(static_cast<uint64_t>(64), statValue);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[4]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("sum"), statName);
  EXPECT_EQ(static_cast<uint64_t>(106), statValue);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[5]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("count"), statName);
  EXPECT_EQ(static_cast<uint64_t>(2), statValue);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[6]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("mean"), statName);
  EXPECT_EQ(static_cast<uint64_t>(53), statValue);

  EXPECT_TRUE(RE2::FullMatch(
      std::string(buffer + lineStartIndices[7]), statLineRegex, &statName,
      &statValue));

  EXPECT_EQ(std::string("variance"), statName);
  EXPECT_EQ(static_cast<uint64_t>(121), statValue);

  delete[] buffer;
}
