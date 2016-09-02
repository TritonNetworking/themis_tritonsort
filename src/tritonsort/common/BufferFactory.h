#ifndef THEMIS_BUFFER_FACTORY_H
#define THEMIS_BUFFER_FACTORY_H

#include <limits>
#include <stdint.h>

#include "core/BaseWorker.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/ResourceFactory.h"

/**
   A base factory for buffers that creates default memory allocation contexts
   and handles allocating memory using those contexts
*/
template <typename T> class BufferFactory : public ResourceFactory {
public:
  BufferFactory(
    BaseWorker& _parentWorker, MemoryAllocatorInterface& _memoryAllocator,
    uint64_t _defaultCapacity, uint64_t _alignmentMultiple = 0)
    : defaultCapacity(_defaultCapacity),
      alignmentMultiple(_alignmentMultiple),
      parentWorker(_parentWorker),
      callerID(std::numeric_limits<uint64_t>::max()),
      inited(false),
      memoryAllocator(_memoryAllocator),
      blockingDefaultContext(NULL),
      nonBlockingDefaultContext(NULL) {
  }

  virtual ~BufferFactory() {
    if (blockingDefaultContext != NULL) {
      delete blockingDefaultContext;
    }

    if (nonBlockingDefaultContext != NULL) {
      delete nonBlockingDefaultContext;
    }
  }

  virtual T* newInstance() {
    // Lazily register with the memory allocator
    if (!inited) {
      init();
      inited = true;
    }

    return newInstance(*blockingDefaultContext, defaultCapacity);
  }

  virtual T* newInstance(uint64_t capacity) {
    // Lazily register with the memory allocator
    if (!inited) {
      init();
      inited = true;
    }

    MemoryAllocationContext context(
      callerID, capacity + alignmentMultiple, false);

    return newInstance(context, capacity);
  }

  virtual T* attemptNewInstance() {
    // Lazily register with the memory allocator
    if (!inited) {
      init();
      inited = true;
    }

    return attemptNewInstance(*nonBlockingDefaultContext, defaultCapacity);
  }

  virtual T* attemptNewInstance(uint64_t capacity) {
    // Lazily register with the memory allocator
    if (!inited) {
      init();
      inited = true;
    }

    MemoryAllocationContext context(
      callerID, capacity + alignmentMultiple, true);

    return attemptNewInstance(context, capacity);
  }

  /**
     Create a new buffer sourced from the network. Typically called by a
     receiver before receiving bytes. Metadata may be used to assist in
     buffer creation, as well as the ID of the peer that sent the buffer.
     By default this just creates a default-sized buffer.

     \param metadata metadata sent before the buffer that may contain useful
     information such as buffer size

     \param peerID the ID of the peer that sent this buffer

     \return a new buffer sourced from the network
   */
  virtual T* newInstanceFromNetwork(uint8_t* metadata, uint64_t peerID) {
    return newInstance();
  }

  /**
     Used to tell the receiver how many bytes of metadata to expect before
     creating a buffer from this factory. Factories that need metadata override
     this with their own static method to allow the receiver to wait for the
     appropriate number of bytes.

     \return the number of bytes of metadata to use when constructing buffers
     from the network
   */
  static uint64_t networkMetadataSize() {
    return 0;
  }

  uint64_t getDefaultSize() {
    return defaultCapacity;
  }

protected:
  virtual T* createNewBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple) = 0;

private:
  void init() {
    callerID = memoryAllocator.registerCaller(parentWorker);

    uint64_t defaultBufferSize = defaultCapacity + alignmentMultiple;

    blockingDefaultContext = new MemoryAllocationContext(
      callerID, defaultBufferSize);
    nonBlockingDefaultContext = new MemoryAllocationContext(
      callerID, defaultBufferSize, true);
  }

  T* newInstance(MemoryAllocationContext& context, uint64_t capacity) {
    uint8_t* memoryRegion = static_cast<uint8_t*>(memoryAllocator.allocate(
                                                    context));

    TRITONSORT_ASSERT(memoryRegion != NULL, "Memory allocator returned NULL from a "
           "blocking request");

    return createNewBuffer(
      memoryAllocator, memoryRegion, capacity, alignmentMultiple);
  }

  T* attemptNewInstance(MemoryAllocationContext& context, uint64_t capacity) {
    uint8_t* memoryRegion = static_cast<uint8_t*>(
      memoryAllocator.allocate(context));

    T* buffer = NULL;

    if (memoryRegion != NULL) {
      buffer = createNewBuffer(
        memoryAllocator, memoryRegion, capacity, alignmentMultiple);
    }

    return buffer;
  }

  const uint64_t defaultCapacity;
  const uint64_t alignmentMultiple;

  BaseWorker& parentWorker;
  uint64_t callerID;

  bool inited;

  MemoryAllocatorInterface& memoryAllocator;

  MemoryAllocationContext* blockingDefaultContext;
  MemoryAllocationContext* nonBlockingDefaultContext;
};

#endif //THEMIS_BUFFER_FACTORY_H
