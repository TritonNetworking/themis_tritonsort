#include <limits>

#include "ByteStreamBuffer.h"

ByteStreamBuffer::ByteStreamBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
  uint64_t capacity, uint64_t alignmentMultiple)
  : BaseBuffer(memoryAllocator, callerID, capacity, alignmentMultiple),
    streamID(std::numeric_limits<uint64_t>::max()) {
}

ByteStreamBuffer::ByteStreamBuffer(MemoryAllocatorInterface& memoryAllocator,
                                   uint8_t* memoryRegion,
                                   uint64_t capacity,
                                   uint64_t alignmentMultiple)
  : BaseBuffer(memoryAllocator, memoryRegion, capacity, alignmentMultiple),
    streamID(std::numeric_limits<uint64_t>::max()) {}

uint64_t ByteStreamBuffer::getStreamID() {
  return streamID;
}

void ByteStreamBuffer::setStreamID(uint64_t streamID) {
  this->streamID = streamID;
}
