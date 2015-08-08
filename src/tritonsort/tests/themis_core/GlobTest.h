#ifndef TRITONSORT_GLOB_TEST_H
#define TRITONSORT_GLOB_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class GlobTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( GlobTest );
  CPPUNIT_TEST( testSimpleFileGlob );
  CPPUNIT_TEST( testNoFilesFound );
  CPPUNIT_TEST( testNoWildcards );
  CPPUNIT_TEST( testSimpleDirectoryGlob );
  CPPUNIT_TEST_SUITE_END();

public:
  void setUp();
  void tearDown();

  void testSimpleFileGlob();
  void testNoFilesFound();
  void testNoWildcards();
  void testSimpleDirectoryGlob();
};

#endif // TRITONSORT_GLOB_TEST_H
