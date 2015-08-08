#include <new>

#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"

void handleFailedMemoryAllocation(size_t size) {
  ABORT("Allocation of size %llu failed", size);
}

void* operator new (size_t size, const themis::memcheck_t& memcheck) {
  // Call placement new with std::nothrow
  void* allocdMemory = ::operator new(size, std::nothrow);

  if (allocdMemory == NULL) {
    handleFailedMemoryAllocation(size);
  }

  return allocdMemory;
}

void* operator new[] (size_t size, const themis::memcheck_t& memcheck) {
  void* allocdMemory = ::operator new[](size, std::nothrow);

  if (allocdMemory == NULL) {
    handleFailedMemoryAllocation(size);
  }

  return allocdMemory;
}
