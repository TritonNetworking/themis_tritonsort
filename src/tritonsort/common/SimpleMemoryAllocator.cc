#include "common/SimpleMemoryAllocator.h"
#include "core/BaseWorker.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryUtils.h"

uint64_t SimpleMemoryAllocator::registerCaller(BaseWorker& caller) {
  /// \todo(AR) This is a kludge designed to avoid concurrent access to a
  /// mapping from caller ID to worker; in future, either the caller ID should
  /// be made to include a reference to its worker, or we should use a
  /// concurrent vector to store the mapping from caller IDs to workers
  return reinterpret_cast<uint64_t>(&caller);
}

uint64_t SimpleMemoryAllocator::registerCaller(
  BaseWorker& caller, const std::string& customGroupName) {
  return registerCaller(caller);
}

void* SimpleMemoryAllocator::allocate(const MemoryAllocationContext& context) {
  uint64_t dummySize = 0;
  return allocate(context, dummySize);
}

void* SimpleMemoryAllocator::allocate(const MemoryAllocationContext& context,
                                      uint64_t& size) {
  BaseWorker* caller = reinterpret_cast<BaseWorker*>(context.getCallerID());

  caller->startMemoryAllocationTimer();

  size = 0;

  const MemoryAllocationContext::MemorySizes& memSizes = context.getSizes();

  for (MemoryAllocationContext::MemorySizes::const_iterator iter =
         memSizes.begin(); iter != memSizes.end(); iter++) {
    size = std::max<uint64_t>(*iter, size);
  }

  uint8_t* memoryRegion = new (themis::memcheck) uint8_t[size];

  caller->stopMemoryAllocationTimer();
  return memoryRegion;
}

void SimpleMemoryAllocator::deallocate(void* memory) {
  uint8_t* castedMemory = static_cast<uint8_t*>(memory);
  delete[] castedMemory;
}
