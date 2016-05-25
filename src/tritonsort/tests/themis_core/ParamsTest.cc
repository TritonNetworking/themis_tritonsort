#include <boost/filesystem.hpp>

#include "ParamsTest.h"
#include "core/Utils.h"
#include "tests/TestUtils.h"

extern const char* TEST_WRITE_ROOT;
extern const char* TEST_BINARY_ROOT;

void ParamsTest::SetUp() {
  sprintf(LOG_FILE_NAME, "%s/test.log", TEST_WRITE_ROOT);
}

void ParamsTest::TearDown() {
  if (fileExists(LOG_FILE_NAME)) {
    unlink(LOG_FILE_NAME);
  }
}

TEST_F(ParamsTest, testContains) {
  Params params;

  EXPECT_TRUE(!params.contains("FOO"));

  params.add<uint64_t>("FOO", 40);
  EXPECT_TRUE(params.contains("FOO"));

  EXPECT_EQ((uint64_t) 40, params.get<uint64_t>("FOO"));
}

TEST_F(ParamsTest, testContainsWithState) {
  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  const char* testFilename = samplePath.c_str();

  EXPECT_TRUE(fileExists(testFilename));
  Params params;
  params.parseFile(testFilename);

  EXPECT_TRUE(!params.contains("FOO"));
  EXPECT_EQ(std::string("11"), params.get<std::string>("firstline"));
  params.add<uint64_t>("FOO", 40);
  EXPECT_TRUE(params.contains("FOO"));
  EXPECT_EQ((uint64_t) 40, params.get<uint64_t>("FOO"));
}

TEST_F(ParamsTest, testAdd) {
  Params params;
  params.add("foo", std::string("bar"));
  params.add<int>("blah", 47);

  int blah = params.get<int>("blah");
  EXPECT_EQ(47, blah);
}

TEST_F(ParamsTest, testParseCommandLine) {
  Params params;
  char* testArgv[5];
  testArgv[0] = (char*) "programNameHere";
  testArgv[1] = (char*) "-foo";
  testArgv[2] = (char*) "hello";
  testArgv[3] = (char*) "-bar";
  testArgv[4] = (char*) "goodbye";

  int testArgc = 5;

  params.parseCommandLine(testArgc, testArgv);

  EXPECT_TRUE(params.contains("foo"));

  EXPECT_EQ(std::string("hello"), params.get<std::string>("foo"));
  EXPECT_TRUE(params.contains("bar"));
  EXPECT_EQ(std::string("goodbye"), params.get<std::string>("bar"));
}

TEST_F(ParamsTest, testParseFile) {
  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  const char* testFilename = samplePath.c_str();

  EXPECT_TRUE(fileExists(testFilename));

  Params params;
  params.parseFile(testFilename);

  EXPECT_TRUE(params.contains("firstline"));
  EXPECT_EQ(std::string("11"), params.get<std::string>("firstline"));
  EXPECT_TRUE(params.contains("secondline"));
  EXPECT_TRUE(params.contains("secondline"));
  EXPECT_EQ(std::string("gabba gabba hey"),
            params.get<std::string>("secondline"));
  EXPECT_TRUE(params.contains("third_line"));
  EXPECT_TRUE(params.get<bool>("true_value"));
  EXPECT_TRUE(!params.get<bool>("false_value"));
  EXPECT_EQ(16.25, params.get<double>("float_value"));
  EXPECT_EQ(
      static_cast<uint64_t>(0xbadf00d), params.get<uint64_t>("hex_value"));
  EXPECT_EQ(std::string("hola!"), params.get<std::string>(
                         "third_line"));
  EXPECT_TRUE(params.contains("table_value.kittens"));

  ASSERT_THROW(params.get<uint64_t>("table_value"),
               AssertionFailedException);

  EXPECT_EQ(64.4, params.getv<double>("table_value.%s", "float"));

  EXPECT_EQ(
      std::string("fuzzy"), params.get<std::string>("table_value.kittens"));
  EXPECT_TRUE(params.contains("table_value.float"));
  EXPECT_EQ(64.4, params.get<double>("table_value.float"));

  ASSERT_THROW(params.get<std::string>("table_value.answer"),
               AssertionFailedException);
  EXPECT_TRUE(params.contains("table_value.answer.life"));
  EXPECT_EQ(static_cast<uint64_t>(42), params.get<uint64_t>(
      "table_value.answer.life"));
  EXPECT_TRUE(params.contains("table_value.answer.universe"));
  EXPECT_EQ(static_cast<uint64_t>(42), params.get<uint64_t>(
      "table_value.answer.universe"));
  EXPECT_TRUE(params.contains("table_value.answer.everything"));
  EXPECT_EQ(static_cast<uint64_t>(42), params.get<uint64_t>(
      "table_value.answer.everything"));
  EXPECT_TRUE(!params.contains("table_value.missing"));
}

TEST_F(ParamsTest, testGet) {
  Params params;
  params.add("test1", std::string("2"));
  uint64_t test1 = params.get<uint64_t>("test1");
  EXPECT_EQ((uint64_t) 2, test1);
}

TEST_F(ParamsTest, testGetAsString) {
  Params params;

  params.add<uint64_t>("someNumber", 63);
  std::string someNumberAsString = params.get<std::string>("someNumber");

  EXPECT_EQ(std::string("63"), someNumberAsString);
}

TEST_F(ParamsTest, testParseCommandBeforeFile) {
  Params params;
  params.add("firstline", std::string("this should be overwritten"));

  boost::filesystem::path samplePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / "sample.txt");

  params.parseFile(samplePath.c_str());

  EXPECT_EQ(std::string("11"), params.get<std::string>("firstline"));
}

TEST_F(ParamsTest, testParseCommandLineWithConfig) {
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
  EXPECT_EQ((uint64_t) 24, params.get<uint64_t>("firstline"));

  EXPECT_EQ(std::string("gabba gabba hey"),
            params.get<std::string>("secondline"));
  EXPECT_EQ(std::string("hola!"), params.get<std::string>("third_line"));

  for (uint64_t i = 0; i < 5; i++) {
    delete[] argv[i];
  }
}

TEST_F(ParamsTest, testDump) {
  Params oldParams;

  std::string testString = "FOOOOOOOBAR";

  oldParams.add<uint64_t>("BLAH_1", 42);
  oldParams.add<double>("BLAH_2", 42.36);
  oldParams.add<uint64_t>("BLAH_1", 72);
  oldParams.add<std::string>("TEST_BLAH_BLAH", testString);

  oldParams.dump(LOG_FILE_NAME);

  Params params;
  params.parseFile(LOG_FILE_NAME);

  EXPECT_TRUE(params.contains("BLAH_1"));
  EXPECT_EQ((uint64_t) 72, params.get<uint64_t>("BLAH_1"));

  EXPECT_TRUE(params.contains("BLAH_2"));
  EXPECT_EQ((double) 42.36, params.get<double>("BLAH_2"));

  EXPECT_TRUE(params.contains("TEST_BLAH_BLAH"));
  EXPECT_EQ(std::string(testString),
            params.get<std::string>("TEST_BLAH_BLAH"));
}

TEST_F(ParamsTest, testBadCast) {
  Params params;
  params.add<std::string>("BAD_NUMBER", "Fortee twoooooo");
  ASSERT_THROW(params.get<uint64_t>("BAD_NUMBER"),
               AssertionFailedException);
}


TEST_F(ParamsTest, testMissingValueAtFront) {
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

  ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

TEST_F(ParamsTest, testMissingValueAtBack) {
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

  ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

TEST_F(ParamsTest, testMissingValueInMiddle) {
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

  ASSERT_THROW(
    params.parseCommandLine(testArgc, testArgv), AssertionFailedException);
}

TEST_F(ParamsTest, testNegativeNumberValueParsing) {
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

  EXPECT_TRUE(params.contains("MYPEERID"));
  EXPECT_TRUE(params.contains("SOME_NEGATIVE_VALUE"));
  EXPECT_TRUE(params.contains("SKIP_PHASE_ONE"));
  EXPECT_TRUE(params.contains("COORDINATOR.PORT"));


  EXPECT_EQ((uint64_t) 0, params.get<uint64_t>("MYPEERID"));
  EXPECT_EQ((int64_t) -536, params.get<int64_t>("SOME_NEGATIVE_VALUE"));
  EXPECT_EQ((uint64_t) 1, params.get<uint64_t>("SKIP_PHASE_ONE"));
  EXPECT_EQ((uint64_t) 6379, params.get<uint64_t>("COORDINATOR.PORT"));
}
