#include "StatLoggerTest.h"
#include "core/Params.h"
#include "core/StatLogger.h"

void StatLoggerTest::testLoggingToUnregisteredKeyThrowsException() {
  StatLogger testLogger("testLogger");

  CPPUNIT_ASSERT_THROW(testLogger.add(42, 610), AssertionFailedException);
}
