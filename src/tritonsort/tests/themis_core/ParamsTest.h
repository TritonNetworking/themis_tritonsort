#ifndef _TRITONSORT_TEST_PARAMSTEST_H
#define _TRITONSORT_TEST_PARAMSTEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include <string>

#include "core/Params.h"

class ParamsTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ParamsTest );
  CPPUNIT_TEST( testAdd );
  CPPUNIT_TEST( testContains );
  CPPUNIT_TEST( testContainsWithState );
  CPPUNIT_TEST( testParseCommandLine );
  CPPUNIT_TEST( testParseCommandLineWithConfig );
  CPPUNIT_TEST( testParseFile );
  CPPUNIT_TEST( testGet );
  CPPUNIT_TEST( testGetAsString );
  CPPUNIT_TEST( testParseCommandBeforeFile );
  CPPUNIT_TEST( testDump );
  CPPUNIT_TEST( testBadCast );

  CPPUNIT_TEST( testMissingValueAtFront );
  CPPUNIT_TEST( testMissingValueAtBack );
  CPPUNIT_TEST( testMissingValueInMiddle );
  CPPUNIT_TEST( testNegativeNumberValueParsing );

  CPPUNIT_TEST_SUITE_END();

public:
  void setUp();
  void testContains();
  void testContainsWithState();
  void tearDown();
  void testAdd();
  void testParseCommandLine();
  void testParseCommandLineWithConfig();
  void testParseFile();
  void testGet();
  void testGetAsString();
  void testParseCommandBeforeFile();
  void testDump();
  void testBadCast();
  void testMissingValueAtFront();
  void testMissingValueAtBack();
  void testMissingValueInMiddle();
  void testNegativeNumberValueParsing();

private:
  char LOG_FILE_NAME[2550];
};
#endif // _TRITONSORT_TEST_PARAMSTEST_H
