#include "ListableKVPairBuffer.h"

ListableKVPairBuffer::ListableKVPairBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t capacity, uint64_t alignmentMultiple)
  : KVPairBuffer(memoryAllocator, memoryRegion, capacity, alignmentMultiple) {
  prev = NULL;
  next = NULL;
}
