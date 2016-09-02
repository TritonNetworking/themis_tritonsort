#include "core/BaseWorker.h"
#include "core/CachingMemoryAllocator.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"

CachingMemoryAllocator::CachingMemoryAllocator(
  uint64_t _cachedRegionSize, uint64_t _numCachedRegions)
  : cachedRegionSize(_cachedRegionSize),
    numCachedRegions(_numCachedRegions) {

  for (uint64_t i = 0; i < numCachedRegions; i++) {
    uint8_t* region = new (themis::memcheck) uint8_t[cachedRegionSize];
    cache.push(region);
  }
}

CachingMemoryAllocator::~CachingMemoryAllocator() {
  ABORT_IF(cache.size() != numCachedRegions, "Expected all cached regions to "
           "be returned before destruction time, but %llu are outstanding",
           numCachedRegions - cache.size());

  while (!cache.empty()) {
    uint8_t* region = static_cast<uint8_t*>(cache.blockingPop());
    delete[] region;
  }
}

uint64_t CachingMemoryAllocator::registerCaller(BaseWorker& caller) {
  // Kludge to get the parent worker back in allocate. See
  // SimpleMemoryAllocator.
  return reinterpret_cast<uint64_t>(&caller);
}

void* CachingMemoryAllocator::allocate(const MemoryAllocationContext& context) {
  uint64_t dummySize = 0;
  return allocate(context, dummySize);
}

void* CachingMemoryAllocator::allocate(
  const MemoryAllocationContext& context, uint64_t& size) {

  BaseWorker* caller = reinterpret_cast<BaseWorker*>(context.getCallerID());

  caller->startMemoryAllocationTimer();

  TRITONSORT_ASSERT(context.getSizes().size() == 1 &&
         context.getSizes().front() == cachedRegionSize,
         "CachingMemoryAllocator expects caller to only ask for memory regions "
         "whose size is the size being cached");

  size = cachedRegionSize;

  void* region = NULL;
  if (context.getFailIfMemoryNotAvailableImmediately()) {
    // Non-blocking pop may return NULL
    cache.pop(region);
  } else {
    region = cache.blockingPop();
  }

  caller->stopMemoryAllocationTimer();

  return region;
}

void CachingMemoryAllocator::deallocate(void* memory) {
  cache.push(memory);
}
