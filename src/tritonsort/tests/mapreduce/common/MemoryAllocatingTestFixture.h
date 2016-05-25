#ifndef THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE
#define THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>

#include "common/DummyWorker.h"
#include "core/MemoryAllocatorInterface.h"

class MemoryAllocatingTestFixture : public CppUnit::TestFixture {
public:
  MemoryAllocatingTestFixture();
  virtual ~MemoryAllocatingTestFixture();

protected:
  DummyWorker dummyParentWorker;
  MemoryAllocatorInterface* memoryAllocator;
  uint64_t callerID;
};
#endif // THEMIS_MEMORY_ALLOCATING_TEST_FIXTURE
