#ifndef THEMIS_RECORD_FILTER_TEST_H
#define THEMIS_RECORD_FILTER_TEST_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>

class RecordFilterTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( RecordFilterTest );
  CPPUNIT_TEST( testLoadFromMap );
  CPPUNIT_TEST_SUITE_END();
public:
  void testLoadFromMap();
};

#endif // THEMIS_RECORD_FILTER_TEST_H
