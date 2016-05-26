#include "mapreduce/common/sorting/RadixSortStrategy.h"
#include "tests/mapreduce/common/sorting/RadixSortStrategyTests.h"

void RadixSortStrategyTests::testUniformSize(
  uint64_t numRecords, uint64_t keyLength, uint64_t valueLength,
  bool secondaryKeys) {

  RadixSortStrategy strategy(secondaryKeys);

  testUniformSizeRecords(
    strategy, numRecords, keyLength, valueLength, secondaryKeys);
}

TEST_F(RadixSortStrategyTests, testNormal) {
  testUniformSize(5000, 10, 90, false);
}

TEST_F(RadixSortStrategyTests, testSecondaryKeys) {
  testUniformSize(5000, 10, 90, true);
}
