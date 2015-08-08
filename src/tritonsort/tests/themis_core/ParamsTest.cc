#include <boost/filesystem.hpp>

#include "ParamsTest.h"
#include "core/Utils.h"
#include "tests/TestUtils.h"

extern const char* TEST_WRITE_ROOT;
extern const char* TEST_BINARY_ROOT;

void ParamsTest::setUp() {
  sprintf(LOG_FILE_NAME, "%s/test.log", TEST_WRITE_ROOT);
}

void ParamsTest::tearDown() {
  if (fileExists(LOG_FILE_NAME)) {
    unlink(LOG_FILE_NAME);
  }
}

void ParamsTest::testContains() {
  Params params;

  CPPUNIT_ASSERT(!params.contains("FOO"));

  params.add<uint64_t>("FOO", 40);
  CPPUNIT_ASSERT(params.contains("FOO"));

  CPPUNIT_ASSERT_EQUAL((uint64_t) 40, params.get<uint64_t>("FOO"));
}

void ParamsTest::testContainsWithState() {
  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  const char* testFilename = samplePath.c_str();

  CPPUNIT_ASSERT(fileExists(testFilename));
  Params params;
  params.parseFile(testFilename);

  CPPUNIT_ASSERT(!params.contains("FOO"));
  CPPUNIT_ASSERT_EQUAL(std::string("11"), params.get<std::string>("firstline"));
  params.add<uint64_t>("FOO", 40);
  CPPUNIT_ASSERT(params.contains("FOO"));
  CPPUNIT_ASSERT_EQUAL((uint64_t) 40, params.get<uint64_t>("FOO"));
}

void ParamsTest::testAdd() {
  Params params;
  params.add("foo", std::string("bar"));
  params.add<int>("blah", 47);

  int blah = params.get<int>("blah");
  CPPUNIT_ASSERT_EQUAL(47, blah);
}

void ParamsTest::testParseCommandLine() {
  Params params;
  char* testArgv[5];
  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-foo";
  testArgv[2] = (char*) "hello";
  testArgv[3] = (char*) "-bar";
  testArgv[4] = (char*) "goodbye";

  int testArgc = 5;

  params.parseCommandLine(testArgc, testArgv);

  CPPUNIT_ASSERT(params.contains("foo"));

  CPPUNIT_ASSERT_EQUAL(std::string("hello"), params.get<std::string>("foo"));
  CPPUNIT_ASSERT(params.contains("bar"));
  CPPUNIT_ASSERT_EQUAL(std::string("goodbye"), params.get<std::string>("bar"));
}

void ParamsTest::testParseFile() {
  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  const char* testFilename = samplePath.c_str();

  CPPUNIT_ASSERT(fileExists(testFilename));

  Params params;
  params.parseFile(testFilename);

  CPPUNIT_ASSERT(params.contains("firstline"));
  CPPUNIT_ASSERT_EQUAL(std::string("11"), params.get<std::string>("firstline"));
  CPPUNIT_ASSERT(params.contains("secondline"));
  CPPUNIT_ASSERT(params.contains("secondline"));
  CPPUNIT_ASSERT_EQUAL(std::string("gabba gabba hey"),
                       params.get<std::string>("secondline"));
  CPPUNIT_ASSERT(params.contains("third_line"));
  CPPUNIT_ASSERT(params.get<bool>("true_value"));
  CPPUNIT_ASSERT(!params.get<bool>("false_value"));
  CPPUNIT_ASSERT_EQUAL(16.25, params.get<double>("float_value"));
  CPPUNIT_ASSERT_EQUAL(
    static_cast<uint64_t>(0xbadf00d), params.get<uint64_t>("hex_value"));
  CPPUNIT_ASSERT_EQUAL(std::string("hola!"), params.get<std::string>(
                         "third_line"));
  CPPUNIT_ASSERT(params.contains("table_value.kittens"));

  CPPUNIT_ASSERT_THROW(params.get<uint64_t>("table_value"),
                       AssertionFailedException);

  CPPUNIT_ASSERT_EQUAL(64.4, params.getv<double>("table_value.%s", "float"));

  CPPUNIT_ASSERT_EQUAL(
    std::string("fuzzy"), params.get<std::string>("table_value.kittens"));
  CPPUNIT_ASSERT(params.contains("table_value.float"));
  CPPUNIT_ASSERT_EQUAL(64.4, params.get<double>("table_value.float"));

  CPPUNIT_ASSERT_THROW(params.get<std::string>("table_value.answer"),
                       AssertionFailedException);
  CPPUNIT_ASSERT(params.contains("table_value.answer.life"));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), params.get<uint64_t>(
                         "table_value.answer.life"));
  CPPUNIT_ASSERT(params.contains("table_value.answer.universe"));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), params.get<uint64_t>(
                         "table_value.answer.universe"));
  CPPUNIT_ASSERT(params.contains("table_value.answer.everything"));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), params.get<uint64_t>(
                         "table_value.answer.everything"));
  CPPUNIT_ASSERT(!params.contains("table_value.missing"));
}

void ParamsTest::testGet() {
  Params params;
  params.add("test1", std::string("2"));
  uint64_t test1 = params.get<uint64_t>("test1");
  CPPUNIT_ASSERT_EQUAL((uint64_t) 2, test1);
}

void ParamsTest::testGetAsString() {
  Params params;

  params.add<uint64_t>("someNumber", 63);
  std::string someNumberAsString = params.get<std::string>("someNumber");

  CPPUNIT_ASSERT_EQUAL(std::string("63"), someNumberAsString);
}

void ParamsTest::testParseCommandBeforeFile() {
  Params params;
  params.add("firstline", std::string("this should be overwritten"));

  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  params.parseFile(samplePath.c_str());

  CPPUNIT_ASSERT_EQUAL(std::string("11"), params.get<std::string>("firstline"));
}

void ParamsTest::testParseCommandLineWithConfig() {
  Params params;
  int argc = 5;
  char* argv[5];

  for (uint64_t i = 0; i < 5; i++) {
    argv[i] = new char[2550];
    memset(argv[i], 0, 2550);
  }

  memcpy(argv[0], "mytestprogram", sizeof("mytestprogram"));
  memcpy(argv[1], "-CONFIG", sizeof("-CONFIG"));

  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");
  std::string testFilename(samplePath.string());

  memcpy(argv[2], testFilename.c_str(), testFilename.size());
  memcpy(argv[3], "-firstline", sizeof("-firstline"));
  memcpy(argv[4], "24", sizeof("24"));

  params.parseCommandLine(argc, argv);
  // Command-line arguments should take precedence over config file arguments
  CPPUNIT_ASSERT_EQUAL((uint64_t) 24, params.get<uint64_t>("firstline"));

  CPPUNIT_ASSERT_EQUAL(std::string("gabba gabba hey"),
                       params.get<std::string>("secondline"));
  CPPUNIT_ASSERT_EQUAL(std::string("hola!"),
                       params.get<std::string>("third_line"));

  for (uint64_t i = 0; i < 5; i++) {
    delete[] argv[i];
  }
}

void ParamsTest::testDump() {
  Params oldParams;

  std::string testString = "FOOOOOOOBAR";

  oldParams.add<uint64_t>("BLAH_1", 42);
  oldParams.add<double>("BLAH_2", 42.36);
  oldParams.add<uint64_t>("BLAH_1", 72);
  oldParams.add<std::string>("TEST_BLAH_BLAH", testString);

  oldParams.dump(LOG_FILE_NAME);

  Params params;
  params.parseFile(LOG_FILE_NAME);

  CPPUNIT_ASSERT(params.contains("BLAH_1"));
  CPPUNIT_ASSERT_EQUAL((uint64_t) 72, params.get<uint64_t>("BLAH_1"));

  CPPUNIT_ASSERT(params.contains("BLAH_2"));
  CPPUNIT_ASSERT_EQUAL((double) 42.36, params.get<double>("BLAH_2"));

  CPPUNIT_ASSERT(params.contains("TEST_BLAH_BLAH"));
  CPPUNIT_ASSERT_EQUAL(std::string(testString),
                       params.get<std::string>("TEST_BLAH_BLAH"));
}

void ParamsTest::testBadCast() {
  Params params;
  params.add<std::string>("BAD_NUMBER", "Fortee twoooooo");
  CPPUNIT_ASSERT_THROW(params.get<uint64_t>("BAD_NUMBER"),
                       AssertionFailedException);
}


void ParamsTest::testMissingValueAtFront() {
  Params params;

  char* testArgv[9];

  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-MY_IP_ADDRESS";
  testArgv[2] = (char*) "-MYPEERID";
  testArgv[3] = (char*) "0";
  testArgv[4] = (char*) "-COORDINATOR.PORT";
  testArgv[5] = (char*) "6379";
  testArgv[6] = (char*) "-SKIP_PHASE_ONE";
  testArgv[7] = (char*) "1";
  testArgv[8] = (char*) "-PEER_LIST";

  int testArgc = 9;

  CPPUNIT_ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

void ParamsTest::testMissingValueAtBack() {
  Params params;

  char* testArgv[9];

  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-MY_IP_ADDRESS";
  testArgv[2] = (char*) "0";
  testArgv[3] = (char*) "-COORDINATOR.PORT";
  testArgv[4] = (char*) "6379";
  testArgv[5] = (char*) "-SKIP_PHASE_ONE";
  testArgv[6] = (char*) "1";
  testArgv[7] = (char*) "-PEER_LIST";
  testArgv[8] = (char*) "-MYPEERID";

  int testArgc = 9;

  CPPUNIT_ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

void ParamsTest::testMissingValueInMiddle() {
  Params params;

  char* testArgv[9];

  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-MYPEERID";
  testArgv[2] = (char*) "0";
  testArgv[3] = (char*) "-MY_IP_ADDRESS";
  testArgv[4] = (char*) "-SKIP_PHASE_ONE";
  testArgv[5] = (char*) "1";
  testArgv[6] = (char*) "-PEER_LIST";
  testArgv[7] = (char*) "-COORDINATOR.PORT";
  testArgv[8] = (char*) "6379";

  int testArgc = 9;

  CPPUNIT_ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

void ParamsTest::testNegativeNumberValueParsing() {
  Params params;

  char* testArgv[9];

  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-MYPEERID";
  testArgv[2] = (char*) "0";
  testArgv[3] = (char*) "-SOME_NEGATIVE_VALUE";
  testArgv[4] = (char*) "-536";
  testArgv[5] = (char*) "-SKIP_PHASE_ONE";
  testArgv[6] = (char*) "1";
  testArgv[7] = (char*) "-COORDINATOR.PORT";
  testArgv[8] = (char*) "6379";

  int testArgc = 9;

  params.parseCommandLine(testArgc, testArgv);

  CPPUNIT_ASSERT(params.contains("MYPEERID"));
  CPPUNIT_ASSERT(params.contains("SOME_NEGATIVE_VALUE"));
  CPPUNIT_ASSERT(params.contains("SKIP_PHASE_ONE"));
  CPPUNIT_ASSERT(params.contains("COORDINATOR.PORT"));


  CPPUNIT_ASSERT_EQUAL((uint64_t) 0, params.get<uint64_t>("MYPEERID"));
  CPPUNIT_ASSERT_EQUAL(
    (int64_t) -536, params.get<int64_t>("SOME_NEGATIVE_VALUE"));
  CPPUNIT_ASSERT_EQUAL((uint64_t) 1, params.get<uint64_t>("SKIP_PHASE_ONE"));
  CPPUNIT_ASSERT_EQUAL(
    (uint64_t) 6379, params.get<uint64_t>("COORDINATOR.PORT"));


}
