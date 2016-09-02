#include <algorithm>
#include <new>

#include "common/AlignmentUtils.h"
#include "common/buffers/BaseBuffer.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"

BaseBuffer::BaseBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
  uint64_t _capacity, uint64_t _alignmentMultiple)
  : capacity(_capacity),
    currentSize(0),
    pendingAppendPtr(NULL),
    pendingAppendLength(0),
    maximumCapacity(_capacity),
    alignmentMultiple(_alignmentMultiple),
    deleteBufferOnDestruct(true),
    memoryAllocator(&memoryAllocator),
    token(NULL) {

  MemoryAllocationContext context(callerID, capacity + alignmentMultiple);
  bufferMemory = static_cast<uint8_t*>(memoryAllocator.allocate(context));

  // Align the beginning of the buffer for direct IO.
  buffer = align(bufferMemory, alignmentMultiple);
}

BaseBuffer::BaseBuffer(uint64_t _capacity, uint64_t _alignmentMultiple)
  : capacity(_capacity),
    currentSize(0),
    pendingAppendPtr(NULL),
    pendingAppendLength(0),
    maximumCapacity(_capacity),
    alignmentMultiple(_alignmentMultiple),
    deleteBufferOnDestruct(true),
    memoryAllocator(NULL),
    bufferMemory(new (themis::memcheck) uint8_t[capacity + alignmentMultiple]),
    buffer(align(bufferMemory, alignmentMultiple)),
    token(NULL) {
}

BaseBuffer::BaseBuffer(
  MemoryAllocatorInterface& _memoryAllocator, uint8_t* memoryRegion,
  uint64_t _capacity, uint64_t _alignmentMultiple)
  : capacity(_capacity),
    currentSize(0),
    pendingAppendPtr(NULL),
    pendingAppendLength(0),
    maximumCapacity(_capacity),
    alignmentMultiple(_alignmentMultiple),
    deleteBufferOnDestruct(true),
    memoryAllocator(&_memoryAllocator),
    bufferMemory(memoryRegion),
    buffer(align(bufferMemory, alignmentMultiple)),
    token(NULL) {
}

BaseBuffer::BaseBuffer(uint8_t* memoryRegion, uint64_t _capacity,
                       uint64_t _alignmentMultiple)
  : capacity(_capacity),
    currentSize(0),
    pendingAppendPtr(NULL),
    pendingAppendLength(0),
    maximumCapacity(_capacity),
    alignmentMultiple(_alignmentMultiple),
    deleteBufferOnDestruct(false),
    memoryAllocator(NULL),
    bufferMemory(memoryRegion),
    buffer(align(bufferMemory, alignmentMultiple)),
    token(NULL) {
}

BaseBuffer::~BaseBuffer() {
  deleteBuffer();
}

void BaseBuffer::stealMemory(BaseBuffer& otherBuffer) {
  // Delete the buffer we currently have so we don't leak.
  deleteBuffer();

  // Steal the memory from the other buffer.
  bufferMemory = otherBuffer.bufferMemory;
  buffer = otherBuffer.buffer;
  memoryAllocator = otherBuffer.memoryAllocator;
  capacity = otherBuffer.capacity;
  maximumCapacity = otherBuffer.maximumCapacity;
  currentSize = otherBuffer.currentSize;
  alignmentMultiple = otherBuffer.alignmentMultiple;
  pendingAppendPtr = otherBuffer.pendingAppendPtr;
  pendingAppendLength = otherBuffer.pendingAppendLength;
  deleteBufferOnDestruct = otherBuffer.deleteBufferOnDestruct;
  token = otherBuffer.token;

  // Set the other buffer's memory to NULL;
  otherBuffer.bufferMemory = NULL;
  otherBuffer.buffer = NULL;
  otherBuffer.memoryAllocator = NULL;
  otherBuffer.capacity = 0;
  otherBuffer.maximumCapacity = 0;
  otherBuffer.capacity = 0;
  otherBuffer.alignmentMultiple = 0;
  otherBuffer.deleteBufferOnDestruct = false;
  otherBuffer.clear();
}

void BaseBuffer::seekForward(uint64_t offset) {
  TRITONSORT_ASSERT(offset <= capacity,
         "Tried to seek forward %llu bytes, but capacity is %llu", offset,
         capacity);
  buffer += offset;
  capacity -= offset;
  currentSize -= std::min<uint64_t>(currentSize, offset);
}

void BaseBuffer::seekBackward(uint64_t offset) {
  TRITONSORT_ASSERT(capacity + offset <= maximumCapacity,
         "Tried to seek backward %llu bytes, but this would put us before the "
         "start of the buffer by %llu bytes", offset,
         capacity + offset - maximumCapacity);
  buffer -= offset;
  capacity += offset;
  currentSize += offset;
}

const uint8_t* BaseBuffer::setupAppend(uint64_t maximumAppendLength) {
  TRITONSORT_ASSERT(currentSize + maximumAppendLength <= capacity, "The append you're "
         "about to do could append off the end of the buffer. You want %llu "
         "bytes and the buffer has %llu left (%llu / %llu)",
         maximumAppendLength, capacity - currentSize, currentSize, capacity);
  TRITONSORT_ASSERT(pendingAppendPtr == NULL, "Tried to do an append while another append "
         "was outstanding");

  pendingAppendPtr = buffer + currentSize;
  pendingAppendLength = maximumAppendLength;

  return pendingAppendPtr;
}

void BaseBuffer::commitAppend(const uint8_t* ptr, uint64_t actualAppendLength) {
  TRITONSORT_ASSERT(ptr == pendingAppendPtr, "Tried to commit an append using a different "
         "pointer than the one used to setup the append");
  TRITONSORT_ASSERT(actualAppendLength <= pendingAppendLength, "Appended more data to the "
         "buffer than you promised you would.");
  currentSize += actualAppendLength;
  pendingAppendPtr = NULL;
  pendingAppendLength = 0;
}

void BaseBuffer::abortAppend(const uint8_t* ptr) {
  pendingAppendPtr = NULL;
  pendingAppendLength = 0;
}

uint64_t BaseBuffer::getCapacity() const {
  return capacity;
}

uint64_t BaseBuffer::getCurrentSize() const {
  return currentSize;
}

void BaseBuffer::setCurrentSize(uint64_t size) {
  TRITONSORT_ASSERT(size <= capacity, "Can't set size to be bigger than the capacity of "
         "the buffer");
  currentSize = size;
}

const uint8_t* const BaseBuffer::getRawBuffer() const {
  return buffer;
}

void BaseBuffer::fillWithRandomData() {
  // Buffer contains whatever junk memory contains at the time
  if (alignmentMultiple == 0) {
    currentSize = capacity;
  } else {
    currentSize = (capacity / alignmentMultiple) * alignmentMultiple;
  }
}

void BaseBuffer::deleteBuffer() {
  if (deleteBufferOnDestruct && bufferMemory != NULL) {
    // Delete the actual buffer memory regardless of seek position.
    if (memoryAllocator != NULL) {
      memoryAllocator->deallocate(bufferMemory);
    } else {
      delete[] bufferMemory;
    }
  }
}
