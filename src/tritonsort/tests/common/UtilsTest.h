#ifndef _TRITONSORT_UTILSTEST_H
#define _TRITONSORT_UTILSTEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class UtilsTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( UtilsTest );
  CPPUNIT_TEST( testParseCommaDelimitedList );
  CPPUNIT_TEST_SUITE_END();
public:
  void setUp();
  void tearDown();
  void testParseCommaDelimitedList();
};

#endif // _TRITONSORT_UTILSTEST_H
