#include <iostream>
#include <limits>
#include <stdint.h>

#include "MemoryUtilsTest.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"

void MemoryUtilsTest::testNormalAllocation() {
  std::string* fooString = NULL;
  uint8_t* testArray = NULL;

  CPPUNIT_ASSERT_NO_THROW(fooString = new (themis::memcheck) std::string(
                            "I'm a test string woo hoo!"));
  CPPUNIT_ASSERT_NO_THROW(testArray = new (themis::memcheck) uint8_t[64000]);

  if (testArray != NULL) {
    for (uint64_t i = 0; i < 64000; i++) {
      testArray[i] = i;
    }

    delete[] testArray;
  }

  if (fooString != NULL) {
    delete fooString;
  }
}

void MemoryUtilsTest::testReallyHugeAllocationFailsAppropriately() {
  uint8_t* testArray = NULL;

  // Allocating 2^64B of memory is pretty much guaranteed to fail for the next
  // few years
  CPPUNIT_ASSERT_THROW(testArray = new (themis::memcheck) uint8_t[
                         std::numeric_limits<uint64_t>::max()],
                       AssertionFailedException);

  if (testArray != NULL) {
    delete[] testArray;
  }
}
