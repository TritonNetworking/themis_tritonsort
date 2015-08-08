#include <iostream>
#include <re2/re2.h>

#include "tests/themis_core/StatWriterTest.h"

#include "core/File.h"
#include "core/Params.h"
#include "core/StatWriter.h"
#include "core/Utils.h"

extern const char* TEST_WRITE_ROOT;

void StatWriterTest::testNormalOperation() {
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

  CPPUNIT_ASSERT_MESSAGE("Log file shouldn't be empty", fileSize > 0);

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

  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(8), numLines);

  re2::RE2 collLineRegex(
    "COLL\\s+test_phase\\s+0\\s+test_logger\\s+dummy_stat\\s+[0-9]+\\s+"
    "([0-9]+)");

  uint64_t stat;

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 0", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[0]), collLineRegex, &stat));

  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), stat);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 1", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[1]), collLineRegex, &stat));

  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(64), stat);

  re2::RE2 statLineRegex("SUMM\\s+test_phase\\s+0\\s+test_logger\\s+dummy_stat"
                         "\\s+(.*?)\\s+([0-9]+)");

  std::string statName;
  uint64_t statValue;

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 2", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[2]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("min"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), statValue);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 3", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[3]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("max"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(64), statValue);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 4", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[4]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("sum"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(106), statValue);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 5", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[5]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("count"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(2), statValue);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 6", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[6]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("mean"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(53), statValue);

  CPPUNIT_ASSERT_MESSAGE(
    "Incorrect log line 7", re2::RE2::FullMatch(
      std::string(buffer + lineStartIndices[7]), statLineRegex, &statName,
    &statValue));

  CPPUNIT_ASSERT_EQUAL(std::string("variance"), statName);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(121), statValue);

  delete[] buffer;
}
