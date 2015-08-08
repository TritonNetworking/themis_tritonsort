#include "MemoryAllocationContext.h"

MemoryAllocationContext::MemoryAllocationContext(
  uint64_t _callerID, uint64_t size,
  bool _failIfMemoryNotAvailableImmediately)
  : callerID(_callerID),
    failIfMemoryNotAvailableImmediately(_failIfMemoryNotAvailableImmediately) {
  sizes.push_back(size);
}

void MemoryAllocationContext::addSize(uint64_t size) {
  sizes.push_back(size);
}

const MemoryAllocationContext::MemorySizes&
MemoryAllocationContext::getSizes() const {
  return sizes;
}

const uint64_t MemoryAllocationContext::getCallerID() const {
  return callerID;
}

bool MemoryAllocationContext::getFailIfMemoryNotAvailableImmediately() const {
  return failIfMemoryNotAvailableImmediately;
}
