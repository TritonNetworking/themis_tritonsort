#ifndef THEMIS_SIMPLE_MEMORY_ALLOCATOR_H
#define THEMIS_SIMPLE_MEMORY_ALLOCATOR_H

#include "core/MemoryAllocatorInterface.h"

/**
   This memory allocator is "simple" in that it does no scheduling, provides no
   memory bound checking, and picks the largest allocation size when given
   a choice between sizes.
 */
class SimpleMemoryAllocator : public MemoryAllocatorInterface {
public:
  /**
     This method is a no-op, since we don't care about caller IDs for an
     allocator this simple.
   */
  uint64_t registerCaller(BaseWorker& caller);

  /**
     This method is a no-op, since we don't care about caller IDs for an
     allocator this simple.
   */
  uint64_t registerCaller(
    BaseWorker& caller, const std::string& customGroupName);

  /**
     Allocates memory with C++'s built-in new operator

     \sa MemoryAllocatorInterface::allocate
   */
  void* allocate(const MemoryAllocationContext& context);

  /**
     Allocates memory with C++'s built-in new operator

     \sa MemoryAllocatorInterface::allocate(const MemoryAllocationContext&
     context, uint64_t& size)
   */
  void* allocate(const MemoryAllocationContext& context, uint64_t& size);

  /**
     Deallocates memory with C++'s built-in delete[] operator

     \sa MemoryAllocatorInterface::deallocate
   */
  void deallocate(void* memory);
};

#endif // THEMIS_SIMPLE_MEMORY_ALLOCATOR_H
