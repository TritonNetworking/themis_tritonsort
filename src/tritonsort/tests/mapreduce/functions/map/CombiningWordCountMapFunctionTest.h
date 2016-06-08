#ifndef MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H
#define MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H

#include <list>
#include "googletest.h"

class CombiningWordCountMapFunctionTest : public ::testing::Test {
protected:
  void assertListsEqual(
    std::list<uint64_t>& expected, std::list<uint64_t>& received);
};

#endif // MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H
