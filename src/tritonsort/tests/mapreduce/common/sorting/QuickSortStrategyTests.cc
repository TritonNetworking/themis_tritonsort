#include "core/MemoryUtils.h"
#include "mapreduce/common/sorting/QuickSortStrategy.h"
#include "tests/mapreduce/common/sorting/QuickSortStrategyTests.h"

void QuickSortStrategyTests::testWithSecondaryKeys() {
  // Same as testNormal, but check secondary keys
  testUniformRecordSizeBuffer(5000, 10, 90, true);
}

void QuickSortStrategyTests::testUniformRecordSizeBuffer(
  uint64_t numRecords, uint64_t keyLength, uint64_t valueLength,
  bool secondaryKeys) {

  QuickSortStrategy strategy(secondaryKeys);

  testUniformSizeRecords(
    strategy, numRecords, keyLength, valueLength, secondaryKeys);
}


void QuickSortStrategyTests::testNormal() {
  // Run with 5000 records, with key length 10 and value length 90
  testUniformRecordSizeBuffer(5000, 10, 90, false);
}

