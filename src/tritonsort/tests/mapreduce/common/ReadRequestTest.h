#ifndef THEMIS_MAPRED_READ_REQUEST_TEST_H
#define THEMIS_MAPRED_READ_REQUEST_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class ReadRequestTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ReadRequestTest );
  CPPUNIT_TEST( testConstructFromURL );
  CPPUNIT_TEST_SUITE_END();
public:
  void testConstructFromURL();
};

#endif // THEMIS_MAPRED_READ_REQUEST_TEST_H
