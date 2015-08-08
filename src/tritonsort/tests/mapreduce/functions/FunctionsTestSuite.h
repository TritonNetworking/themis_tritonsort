#ifndef MAPRED_FUNCTIONS_TEST_SUITE_H
#define MAPRED_FUNCTIONS_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "map/MapFunctionsTestSuite.h"

class FunctionsTestSuite : public CppUnit::TestSuite {
public:
  FunctionsTestSuite() : CppUnit::TestSuite("MapReduce functions") {
    addTest(new MapFunctionsTestSuite());
  }
};

#endif // MAPRED_FUNCTIONS_TEST_SUITE_H
