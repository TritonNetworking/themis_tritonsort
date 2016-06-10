#ifndef THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H
#define THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H

#include "third-party/googletest.h"

class NGramMapFunctionTest : public ::testing::Test {
protected:
  void insertExpectedKey(std::set<std::string>& expectedKeys, const char* key);
};

#endif // THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H
