#ifndef MAPRED_SORTING_TEST_SUITE_H
#define MAPRED_SORTING_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "tests/mapreduce/common/sorting/QuickSortStrategyTests.h"
#include "tests/mapreduce/common/sorting/RadixSortStrategyTests.h"

class SortingTestSuite : public CppUnit::TestSuite {
public:
  SortingTestSuite() : CppUnit::TestSuite("MapReduce sort strategies") {
    addTest(QuickSortStrategyTests::suite());
    addTest(RadixSortStrategyTests::suite());
  }
};

#endif // MAPRED_SORTING_TEST_SUITE_H
