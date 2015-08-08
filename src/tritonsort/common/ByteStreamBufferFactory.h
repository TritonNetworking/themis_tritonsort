#ifndef MAPRED_BYTE_STREAM_BUFFER_FACTORY_H
#define MAPRED_BYTE_STREAM_BUFFER_FACTORY_H

#include "common/BufferFactory.h"
#include "common/buffers/ByteStreamBuffer.h"

class BaseWorker;
class MemoryAllocatorInterface;

/**
   A factory that can be used to create ByteStreamBuffers
 */
class ByteStreamBufferFactory : public BufferFactory<ByteStreamBuffer> {
public:
  /// Constructor
  /**
     \param parentWorker the worker that will use this factory to create
     buffers

     \param memoryAllocator a memory allocator that will be used to create the
     underlying memory regions for buffers instantiated by this factory

     \param defaultCapacity the default capacity of buffers created by this
     factory

     \param alignmentMultiple buffers created by this factory use memory that
     is aligned to a multiple of this number
   */
  ByteStreamBufferFactory(
    BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
    uint64_t defaultCapacity, uint64_t alignmentMultiple = 0);

protected:
  /// \sa BufferFactory::createNewBuffer
  ByteStreamBuffer* createNewBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple);
};

#endif // MAPRED_BYTE_STREAM_BUFFER_FACTORY_H
