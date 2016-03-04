#ifndef MAP_FUNCTIONS_TEST_SUITE_H
#define MAP_FUNCTIONS_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "BytesCountMapFunctionTest.h"
#include "CombiningWordCountMapFunctionTest.h"
#include "NGramMapFunctionTest.h"

class MapFunctionsTestSuite : public CppUnit::TestSuite {
public:
  MapFunctionsTestSuite() : CppUnit::TestSuite("Map functions") {
    addTest(BytesCountMapFunctionTest::suite());
    addTest(CombiningWordCountMapFunctionTest::suite());
    addTest(NGramMapFunctionTest::suite());
  }
};

#endif // MAP_FUNCTIONS_TEST_SUITE_H
