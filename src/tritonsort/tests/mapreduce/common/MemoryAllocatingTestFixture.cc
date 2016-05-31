#include "common/SimpleMemoryAllocator.h"
#include "tests/mapreduce/common/MemoryAllocatingTestFixture.h"

MemoryAllocatingTestFixture::MemoryAllocatingTestFixture()
  : dummyParentWorker(0, "test") {
  memoryAllocator = new SimpleMemoryAllocator();
  callerID = memoryAllocator->registerCaller(dummyParentWorker);
}

MemoryAllocatingTestFixture::~MemoryAllocatingTestFixture() {
  if (memoryAllocator != NULL) {
    delete memoryAllocator;
  }
}
