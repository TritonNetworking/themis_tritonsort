#ifndef MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H
#define MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <list>

class CombiningWordCountMapFunctionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( CombiningWordCountMapFunctionTest );
  CPPUNIT_TEST(testProperCombination);
  CPPUNIT_TEST(testTooManyKeysToCache);
  CPPUNIT_TEST_SUITE_END();
public:
  void testProperCombination();
  void testTooManyKeysToCache();

private:
  void assertListsEqual(
    std::list<uint64_t>& expected, std::list<uint64_t>& received);
};

#endif // MAPRED_COMBINING_WORD_COUNT_MAP_FUNCTION_TEST_H
