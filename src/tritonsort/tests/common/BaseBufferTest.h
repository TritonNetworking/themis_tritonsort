#ifndef THEMIS_BASE_BUFFER_TEST_H
#define THEMIS_BASE_BUFFER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class BaseBufferTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( BaseBufferTest );
  CPPUNIT_TEST(testAlignment);

#ifdef TRITONSORT_ASSERT
  CPPUNIT_TEST(testSeek);
  CPPUNIT_TEST(testAppend);
  CPPUNIT_TEST(testSetupCommitAppend);
#endif //TRITONSORT_ASSERT

  CPPUNIT_TEST_SUITE_END();

public:
  /// Test buffer alignment for O_DIRECT
  void testAlignment();

  /// Test seeking forward and backward
  void testSeek();

  /// Test appending with the append() call
  void testAppend();

  /// Test appending with setupAppend()/commitAppend()
  void testSetupCommitAppend();
};

#endif // THEMIS_BASE_BUFFER_TEST_H
