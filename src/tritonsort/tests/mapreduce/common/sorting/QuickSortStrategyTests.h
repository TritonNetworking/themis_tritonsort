#ifndef THEMIS_QUICK_SORT_STRATEGY_TEST_H
#define THEMIS_QUICK_SORT_STRATEGY_TEST_H

#include "tests/mapreduce/common/sorting/SortStrategyTestSuite.h"

class KVPairBuffer;

class QuickSortStrategyTests : public SortStrategyTestSuite {
  CPPUNIT_TEST_SUITE( QuickSortStrategyTests );
  CPPUNIT_TEST( testNormal );
  CPPUNIT_TEST( testWithSecondaryKeys );
  CPPUNIT_TEST_SUITE_END();
public:
  void testWithSecondaryKeys();
  void testNormal();

private:
  void testUniformRecordSizeBuffer(
    uint64_t numRecords, uint64_t keyLength, uint64_t valueLength,
    bool secondaryKeys);
};

#endif // THEMIS_QUICK_SORT_STRATEGY_TEST_H
