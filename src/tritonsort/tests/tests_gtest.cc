#include <boost/filesystem.hpp>
#include <iostream>
#include <limits>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include "gtest/gtest.h"

#include "common/MainUtils.h"
#include "config.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/constants.h"

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

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
