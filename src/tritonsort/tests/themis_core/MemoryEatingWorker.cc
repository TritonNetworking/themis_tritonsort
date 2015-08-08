#include "MemoryEatingWorker.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocatorInterface.h"

MemoryEatingWorker::MemoryEatingWorker(
  uint64_t id, const std::string& stageName,
  MemoryAllocatorInterface& allocator)
  : SingleUnitRunnable<UInt64Resource>(id, stageName),
    memoryAllocator(allocator),
    callerID(allocator.registerCaller(*this)) {

}

void MemoryEatingWorker::run(UInt64Resource* resource) {
  uint64_t memorySize = resource->asUInt64();
  delete resource;

  MemoryAllocationContext context(callerID, memorySize, false);
  uint8_t* memory = static_cast<uint8_t*>(memoryAllocator.allocate(context));
  allocatedMemory.push(memory);
}

void MemoryEatingWorker::teardown() {
  while (!allocatedMemory.empty()) {
    uint8_t* memory = allocatedMemory.front();
    allocatedMemory.pop();

    memoryAllocator.deallocate(memory);
  }
}

BaseWorker* MemoryEatingWorker::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  MemoryEatingWorker* worker = new MemoryEatingWorker(
    id, stageName, memoryAllocator);

  return worker;
}
