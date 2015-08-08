#ifndef THEMIS_STAT_WRITER_TEST_H
#define THEMIS_STAT_WRITER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class StatWriterTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( StatWriterTest );
  CPPUNIT_TEST( testNormalOperation );
  CPPUNIT_TEST_SUITE_END();

public:
  void testNormalOperation();
};

#endif // THEMIS_STAT_WRITER_TEST_H
