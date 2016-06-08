#ifndef THEMIS_BYTE_ORDER_TEST_H
#define THEMIS_BYTE_ORDER_TEST_H

#include "googletest.h"

class ByteOrderTest : public ::testing::Test {
public:
  ByteOrderTest();

  uint64_t deadbeef;

  uint64_t deadbeefBigEndian;
  uint64_t deadbeefLittleEndian;
  uint64_t deadbeefHostOrder;
};

#endif // THEMIS_BYTE_ORDER_TEST_H
