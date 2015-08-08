#include "core/MemoryUtils.h"
#include "mapreduce/common/ListableKVPairBufferFactory.h"

ListableKVPairBufferFactory::ListableKVPairBufferFactory(
  BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
  uint64_t defaultSize, uint64_t alignmentMultiple)
  : BufferFactory<ListableKVPairBuffer>(
    parentWorker, memoryAllocator, defaultSize, alignmentMultiple) {
}

ListableKVPairBuffer* ListableKVPairBufferFactory::createNewBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t capacity, uint64_t alignmentMultiple) {

  ListableKVPairBuffer* buffer = new (themis::memcheck) ListableKVPairBuffer(
    memoryAllocator, memoryRegion, capacity, alignmentMultiple);

  return buffer;
}
