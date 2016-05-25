#include <boost/filesystem.hpp>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/TextOutputter.h>
#include <cppunit/TextTestProgressListener.h>
#include <cppunit/XmlOutputter.h>
#include <iostream>
#include <limits>
#include <signal.h>
#include <stdlib.h>
#include <string>

#include "common/MainUtils.h"
#include "config.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/constants.h"
#include "tests/mapreduce/MapReduceTestSuite.h"


const char* TEST_WRITE_ROOT;
const char* TEST_BINARY_ROOT;

int main(int argc, char** argv) {
  boost::filesystem::path testBinaryParentDir(
    boost::filesystem::absolute(
      boost::filesystem::path(argv[0]).branch_path()));

  TEST_BINARY_ROOT = testBinaryParentDir.c_str();

  signal(SIGSEGV, sigsegvHandler);

  TritonSortAssertions::useTestModeAssertions();

#ifndef TRITONSORT_ASSERTS
  std::cerr << std::endl << "WARNING: Assertions disabled. Some tests "
    "will not run." << std::endl << std::endl;
#endif //TRITONSORT_ASSERTS

  Params params;
  params.parseCommandLine(argc, argv);

  params.add<uint64_t>("MEM_PERCENTAGE", 100);
  params.add<uint64_t>("MEM_SIZE", 500000000);

  limitMemorySize(params);

  if (params.contains("TEST_WRITE_ROOT")) {
    std::string writeLoc = params.get<std::string>("TEST_WRITE_ROOT");
    TEST_WRITE_ROOT = writeLoc.c_str();
  } else {
    std::string defaultTestWriteLoc("/tmp/themis_tests");
    TEST_WRITE_ROOT = defaultTestWriteLoc.c_str();
  }

  char command[2550];
  sprintf(command, "mkdir -p %s", TEST_WRITE_ROOT);
  if (system(command) == -1) {
    perror("system()");
    return 1;
  }

  CppUnit::TestResult controller;
  CppUnit::TestResultCollector result;
  controller.addListener(&result);

  CppUnit::TextTestProgressListener progress;
  controller.addListener( &progress );

  CppUnit::TestRunner runner;
  runner.addTest(new MapReduceTestSuite());

  std::string testPath;

  runner.run(controller, testPath);

  CppUnit::Outputter* outputter = NULL;

  if (params.contains("DUMP_XML_FILE")) {
    std::ofstream xmlFileStream(
      params.get<std::string>("DUMP_XML_FILE").c_str());

    outputter = new CppUnit::XmlOutputter(&result, xmlFileStream);
    outputter->write();

    xmlFileStream.close();
  } else {
    outputter = new CppUnit::TextOutputter(&result, std::cerr);
    outputter->write();
  }

  delete outputter;

  return 0;
}
