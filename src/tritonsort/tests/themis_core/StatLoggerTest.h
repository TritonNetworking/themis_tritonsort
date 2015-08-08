#ifndef TRITONSORT_STAT_LOGGER_TEST_H
#define TRITONSORT_STAT_LOGGER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class StatLoggerTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( StatLoggerTest );

#ifdef TRITONSORT_ASSERTS
  CPPUNIT_TEST( testLoggingToUnregisteredKeyThrowsException );
#endif //TRITONSORT_ASSERTS

  CPPUNIT_TEST_SUITE_END();
public:
  void testLoggingToUnregisteredKeyThrowsException();
};

#endif // TRITONSORT_STAT_LOGGER_TEST_H
