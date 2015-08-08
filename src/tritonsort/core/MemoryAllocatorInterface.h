#ifndef THEMIS_MEMORY_ALLOCATOR_INTERFACE_H
#define THEMIS_MEMORY_ALLOCATOR_INTERFACE_H

#include <stdint.h>
#include <string>

class MemoryAllocationContext;
class BaseWorker;

/**
   Provides an interface that all memory allocation systems in Themis must
   implement.

   Memory allocators allow memory requests by different components within a
   Themis application (typically workers) to be scheduled according to
   different policies, allowing memory allocation between components to remain
   fluid while enforcing various capacity constraints and fairness guarantees.

   Any component that allocates and deallocates memory using a memory allocator
   is referred to from the allocator's point of view as a "caller". In order to
   allocate and deallocate memory from an allocator, the caller must retrieve a
   unique ID for itself using MemoryAllocatorInterface::registerCaller. It can
   then use that unique ID to construct MemoryAllocationContexts for its memory
   allocations.
 */
class MemoryAllocatorInterface {
public:
  /// Destructor
  virtual ~MemoryAllocatorInterface() {}

  /// Register a caller with the memory allocator
  /**
     This method is a convenience function for workers that wish to register
     with a memory allocator; it also allows for possible future extraction of
     additional worker-specific information for scheduling purposes.

     \param caller a reference to the caller, which is a BaseWorker

     \return a unique identifier that the caller can use for future
     interactions with this memory allocator
   */
  virtual uint64_t registerCaller(BaseWorker& caller) = 0;

  /// Allocate a region of memory
  /**
     The caller of this function blocks until the memory allocation can be
     satisfied.

     \param context a MemoryAllocationContext containing information about the
     caller and its requirements for the allocation

     \return a new memory region
   */
  virtual void* allocate(const MemoryAllocationContext& context) = 0;


  /// Allocate a region of memory
  /**
     The caller of this function blocks until the memory allocation can be
     satisfied.

     \param context a MemoryAllocationContext containing information about the
     caller and its requirements for the allocation

     \param[out] size the size of the resulting allocation

     \return a new memory region
   */
  virtual void* allocate(
    const MemoryAllocationContext& context, uint64_t& size) = 0;

  /**
     We assume that the provided memory region was allocated by this allocator
     in a previous call to MemoryAllocatorInterface::allocate

     \param memory the memory region to deallocate
   */
  virtual void deallocate(void* memory) = 0;
};

#endif // THEMIS_MEMORY_ALLOCATOR_INTERFACE_H
