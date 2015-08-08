#ifndef THEMIS_WEX_LINK_EXTRACTOR_MAP_FUNCTION_TEST_H
#define THEMIS_WEX_LINK_EXTRACTOR_MAP_FUNCTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <set>

#include "tests/mapreduce/functions/map/WEXLinkExtractorMapFunctionVerifyingWriter.h"

class WEXLinkExtractorMapFunctionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( WEXLinkExtractorMapFunctionTest );
  CPPUNIT_TEST( testLinkExtraction );
  CPPUNIT_TEST_SUITE_END();

public:
  void testLinkExtraction();

private:
  void checkWriter(
    WEXLinkExtractorMapFunctionVerifyingWriter& writer, const std::string& key,
    const std::set<std::string>& values);
};

#endif // THEMIS_WEX_LINK_EXTRACTOR_MAP_FUNCTION_TEST_H
