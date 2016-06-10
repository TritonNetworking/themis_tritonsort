#ifndef THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE
#define THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE

#include <stdint.h>

#include "common/DummyWorker.h"
#include "core/MemoryAllocatorInterface.h"
#include "third-party/googletest.h"

class MemoryAllocatingTestFixture : public ::testing::Test {
public:
  MemoryAllocatingTestFixture();
  virtual ~MemoryAllocatingTestFixture();

protected:
  DummyWorker dummyParentWorker;
  MemoryAllocatorInterface* memoryAllocator;
  uint64_t callerID;
};
#endif // THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE
