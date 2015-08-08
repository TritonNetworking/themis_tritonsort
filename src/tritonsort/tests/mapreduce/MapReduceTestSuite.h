#ifndef MAPRED_MAIN_TEST_SUITE_H
#define MAPRED_MAIN_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "common/MapredCommonTestSuite.h"
#include "functions/FunctionsTestSuite.h"

class MapReduceTestSuite : public CppUnit::TestSuite {
public:
  MapReduceTestSuite() : CppUnit::TestSuite("MapReduce") {
    addTest(new MapredCommonTestSuite());
    addTest(new FunctionsTestSuite());
  }
};

#endif // MAPRED_MAIN_TEST_SUITE_H
