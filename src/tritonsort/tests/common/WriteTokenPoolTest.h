#ifndef THEMIS_WRITE_TOKEN_POOL_TEST_H
#define THEMIS_WRITE_TOKEN_POOL_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class WriteTokenPoolTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( WriteTokenPoolTest );
  CPPUNIT_TEST( testAttemptGetToExhaustion );
  CPPUNIT_TEST_SUITE_END();

public:
  void testAttemptGetToExhaustion();
};

#endif // THEMIS_WRITE_TOKEN_POOL_TEST_H
