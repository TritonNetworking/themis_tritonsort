#include "ByteStreamBufferFactory.h"
#include "core/BaseWorker.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/MemoryUtils.h"

ByteStreamBufferFactory::ByteStreamBufferFactory(
  BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
  uint64_t defaultCapacity, uint64_t alignmentMultiple)
  : BufferFactory(
    parentWorker, memoryAllocator, defaultCapacity, alignmentMultiple) {
}

ByteStreamBuffer* ByteStreamBufferFactory::createNewBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t capacity, uint64_t alignmentMultiple) {

  ByteStreamBuffer* buffer = new (themis::memcheck) ByteStreamBuffer(
    memoryAllocator, memoryRegion, capacity, alignmentMultiple);

  return buffer;
}

