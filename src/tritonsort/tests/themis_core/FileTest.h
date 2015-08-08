#ifndef THEMIS_FILE_TEST_H
#define THEMIS_FILE_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class FileTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( FileTest );
  CPPUNIT_TEST( testPreallocate );
  CPPUNIT_TEST( testTruncateOnClose );
  CPPUNIT_TEST( testOpenNonexistentFileForReading );
  CPPUNIT_TEST( testReadFlags );
  CPPUNIT_TEST( testWriteFlags );
  CPPUNIT_TEST( testClose );
  CPPUNIT_TEST_SUITE_END();
public:
  void testPreallocate();
  void testTruncateOnClose();
  void testOpenNonexistentFileForReading();
  void testReadFlags();
  void testWriteFlags();
  void testClose();
};

#endif // THEMIS_FILE_TEST_H
