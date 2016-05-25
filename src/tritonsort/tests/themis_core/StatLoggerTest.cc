#include "StatLoggerTest.h"
#include "core/Params.h"
#include "core/StatLogger.h"

#ifdef TRITONSORT_ASSERTS
TEST_F(StatLoggerTest, testLoggingToUnregisteredKeyThrowsException) {
  StatLogger testLogger("testLogger");

  ASSERT_THROW(testLogger.add(42, 610), AssertionFailedException);
}
#endif //TRITONSORT_ASSERTS
