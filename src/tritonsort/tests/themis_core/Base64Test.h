#ifndef THEMIS_BASE_64_TEST_H
#define THEMIS_BASE_64_TEST_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>

class Base64Test : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( Base64Test );

  CPPUNIT_TEST( testAgainstKnownEncode );
  CPPUNIT_TEST( testEncodeAndDecode );
  CPPUNIT_TEST( testDecodeAllCodeLetters );

  CPPUNIT_TEST_SUITE_END();
public:
  void testAgainstKnownEncode();
  void testEncodeAndDecode();
  void testDecodeAllCodeLetters();

private:
  // Taken from Wikipedia's Base64 page
  static const char* testString;

};

#endif // THEMIS_BASE_64_TEST_H
