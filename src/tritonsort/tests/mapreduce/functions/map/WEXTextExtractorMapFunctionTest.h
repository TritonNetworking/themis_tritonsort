#ifndef THEMIS_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_TEST_H
#define THEMIS_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <list>
#include <string>

class StringListVerifyingWriter;

class WEXTextExtractorMapFunctionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( WEXTextExtractorMapFunctionTest );
  CPPUNIT_TEST( testTextExtraction );
  CPPUNIT_TEST( testMissingArticleContents );
  CPPUNIT_TEST_SUITE_END();

public:
  /// Test that article text is extracted from the 5th database column.
  void testTextExtraction();

  /// Test that a row that is missing the 5th column is ignored.
  void testMissingArticleContents();

private:
  /**
     Verify that the writer got a given list of keys and values.

     \param writer the map function's writer

     \param keys the expected list of written keys

     \param values the expected list of written values
   */
  void checkWriter(
    StringListVerifyingWriter& writer, const std::list<std::string>& keys,
    const std::list<std::string>& values);
};

#endif // THEMIS_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_TEST_H
