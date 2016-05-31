#ifndef THEMIS_RADIX_SORT_STRATEGY_TESTS_H
#define THEMIS_RADIX_SORT_STRATEGY_TESTS_H

#include "tests/mapreduce/common/sorting/SortStrategyTestSuite.h"

class RadixSortStrategyTests : public SortStrategyTestSuite {
protected:
  void testUniformSize(
    uint64_t numRecords, uint64_t keyLength, uint64_t valueLength,
    bool secondaryKeys);
};

#endif // THEMIS_RADIX_SORT_STRATEGY_TESTS_H
