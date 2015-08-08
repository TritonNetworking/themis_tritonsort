#ifndef THEMIS_MEMORY_EATING_WORKER_H
#define THEMIS_MEMORY_EATING_WORKER_H

#include "core/SingleUnitRunnable.h"
#include "tests/themis_core/UInt64Resource.h"

class MemoryAllocatorInterface;

/**
   MemoryEatingWorker "eats" memory by allocating memory and not freeing it
   until teardown.
 */
class MemoryEatingWorker : public SingleUnitRunnable<UInt64Resource> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the unique ID of this worker within its parent stage

     \param stageName the name of this worker's parent stage

     \param allocator a memory allocator that this stage will use to allocate
     memory
   */
  MemoryEatingWorker(uint64_t id, const std::string& stageName,
                     MemoryAllocatorInterface& allocator);
  /**
     Interprets the given resource as a uint64_t and allocates that much memory
     using the memory allocator.

     \param resource a resource giving the amount of memory, in bytes, that
     this worker should allocate
   */
  void run(UInt64Resource* resource);

  /// Deallocate all allocated memory blocks
  void teardown();

private:
  MemoryAllocatorInterface& memoryAllocator;
  const uint64_t callerID;

  std::queue<uint8_t*> allocatedMemory;
};

#endif // THEMIS_MEMORY_EATING_WORKER_H
