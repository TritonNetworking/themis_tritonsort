#ifndef _TRITONSORT_COMMON_TEST_SUITE_H
#define _TRITONSORT_COMMON_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "tests/common/BaseBufferTest.h"
#include "tests/common/BufferListTest.h"
#include "tests/common/UtilsTest.h"
#include "tests/common/WriteTokenPoolTest.h"

class CommonTestSuite : public CppUnit::TestSuite {
public:
  CommonTestSuite() : CppUnit::TestSuite("Common objects") {
    addTest(BaseBufferTest::suite());
    addTest(BufferListTest::suite());
    addTest(UtilsTest::suite());
    addTest(WriteTokenPoolTest::suite());
  }
};

#endif // _TRITONSORT_COMMON_TEST_SUITE_H
