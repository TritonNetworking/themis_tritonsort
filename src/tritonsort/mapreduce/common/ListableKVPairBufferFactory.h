#ifndef MAPRED_LISTABLE_KV_PAIR_BUFFER_FACTORY_H
#define MAPRED_LISTABLE_KV_PAIR_BUFFER_FACTORY_H

#include <stdint.h>

#include "common/BufferFactory.h"
#include "mapreduce/common/buffers/ListableKVPairBuffer.h"

/**
   A factory for creating listable KV pair buffers used by the TupleDemux.
   This factory supports two instantiator methods: one that constructs default
   sized buffers and one that constructs custom sized buffers.
 */
class ListableKVPairBufferFactory : public BufferFactory<ListableKVPairBuffer> {
public:
  /// Constructor
  /**
     \param parentWorker the worker that is using this factory to allocate
     buffers

     \param memoryAllocator a memory allocator that will be used to create the
     underlying memory regions for buffers instantiated by this factory

     \param defaultSize the default size of a listable buffer

     \param alignmentMultiple if the memory is to be aligned on an integer
     multiple, alignmentMultiple gives the integer multiple alignment
   */
  ListableKVPairBufferFactory(
    BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
    uint64_t defaultSize, uint64_t alignmentMultiple = 0);

protected:
  ListableKVPairBuffer* createNewBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple);
};

#endif // MAPRED_LISTABLE_KV_PAIR_BUFFER_FACTORY_H
