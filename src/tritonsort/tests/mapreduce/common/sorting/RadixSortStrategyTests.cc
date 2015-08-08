#include "mapreduce/common/sorting/RadixSortStrategy.h"
#include "tests/mapreduce/common/sorting/RadixSortStrategyTests.h"

void RadixSortStrategyTests::testUniformSize(
  uint64_t numRecords, uint64_t keyLength, uint64_t valueLength,
  bool secondaryKeys) {

  RadixSortStrategy strategy(secondaryKeys);

  testUniformSizeRecords(
    strategy, numRecords, keyLength, valueLength, secondaryKeys);
}

void RadixSortStrategyTests::testVariableSize(
  uint64_t numRecords, bool secondaryKeys) {

  RadixSortStrategy strategy(secondaryKeys);

  testVariableSizeRecords(strategy, numRecords, secondaryKeys);
}

void RadixSortStrategyTests::testNormal() {
  testUniformSize(5000, 10, 90, false);
}

void RadixSortStrategyTests::testSecondaryKeys() {
  testUniformSize(5000, 10, 90, true);
}

void RadixSortStrategyTests::testKeyPadding() {
  testVariableSize(5000, false);
}

void RadixSortStrategyTests::testSecondaryKeysWithKeyPadding() {
  testVariableSize(5000, true);
}
