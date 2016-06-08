#ifndef MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H
#define MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H

#include "googletest.h"

class BytesCountMapFunctionTest : public ::testing::Test {
public:
  BytesCountMapFunctionTest();
  virtual ~BytesCountMapFunctionTest();

protected:
  static const uint32_t keyLength = 10;
  static const uint32_t valueLength = 90;
  uint8_t* keyBytes;
  uint8_t* valueBytes;
};

#endif // MAPRED_BYTES_COUNT_MAP_FUNCTION_TEST_H
