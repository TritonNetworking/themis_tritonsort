#ifndef THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H
#define THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class NGramMapFunctionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( NGramMapFunctionTest );
  CPPUNIT_TEST( testMapNGrams );
  CPPUNIT_TEST_SUITE_END();
public:
  void testMapNGrams();

private:
  void insertExpectedKey(std::set<std::string>& expectedKeys, const char* key);
};

#endif // THEMIS_MAPRED_NGRAM_MAP_FUNCTION_TEST_H
