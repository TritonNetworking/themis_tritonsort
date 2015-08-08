#ifndef TRITONSORT_BLOCKING_READ_TEST_H
#define TRITONSORT_BLOCKING_READ_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>

class BlockingReadTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( BlockingReadTest );
  CPPUNIT_TEST( testReadWithNonAlignedSize );
  CPPUNIT_TEST_SUITE_END();

public:
  BlockingReadTest();
  void tearDown();
  void testReadWithNonAlignedSize();

private:
  const static uint64_t MAX_FILENAME_SIZE = 2550;

  char nonAlignedFilename[MAX_FILENAME_SIZE];
};

#endif // TRITONSORT_BLOCKING_READ_TEST_H
