#ifndef TRITONSORT_BLOCKING_READ_TEST_H
#define TRITONSORT_BLOCKING_READ_TEST_H

#include <stdint.h>

#include "third-party/googletest.h"

class BlockingReadTest : public ::testing::Test {
public:
  BlockingReadTest();
  void TearDown();

protected:
  const static uint64_t MAX_FILENAME_SIZE = 2550;

  char nonAlignedFilename[MAX_FILENAME_SIZE];
};

#endif // TRITONSORT_BLOCKING_READ_TEST_H
