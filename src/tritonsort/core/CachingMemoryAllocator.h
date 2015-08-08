#ifndef THEMIS_CACHING_MEMORY_ALLOCATOR_H
#define THEMIS_CACHING_MEMORY_ALLOCATOR_H

#include "core/MemoryAllocatorInterface.h"
#include "core/ThreadSafeQueue.h"

class CachingMemoryAllocator : public MemoryAllocatorInterface {
public:
  CachingMemoryAllocator(uint64_t cachedRegionSize, uint64_t numCachedRegions);
  virtual ~CachingMemoryAllocator();

  uint64_t registerCaller(BaseWorker& caller);
  void* allocate(const MemoryAllocationContext& context);
  void* allocate(const MemoryAllocationContext& context, uint64_t& size);
  void deallocate(void* memory);
private:
  const uint64_t cachedRegionSize;
  const uint64_t numCachedRegions;
  ThreadSafeQueue<void*> cache;
};

#endif // THEMIS_CACHING_MEMORY_ALLOCATOR_H
