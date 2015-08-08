#ifndef SOURCE_FILE_RANGES_SET_TEST_H
#define SOURCE_FILE_RANGES_SET_TEST_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>

class SourceFileRangesSetTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( SourceFileRangesSetTest );
  CPPUNIT_TEST( testMergeRanges );
  CPPUNIT_TEST( testMarshal );
  CPPUNIT_TEST( testMergeSets );
  CPPUNIT_TEST( testEquals );
  CPPUNIT_TEST_SUITE_END();

public:
  void testMergeRanges();
  void testMarshal();
  void testMergeSets();
  void testEquals();
};

#endif // SOURCE_FILE_RANGES_SET_TEST_H
