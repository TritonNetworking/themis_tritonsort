#ifndef MAPRED_BUFFER_COMBINER_H
#define MAPRED_BUFFER_COMBINER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   Combiner buffers into a single buffer.
 */
class BufferCombiner : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker's worker ID within its parent stage

     \param name the worker's stage name

     \param memoryAllocator the memory allocator to use for constructing buffers

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size
   */
  BufferCombiner(
    uint64_t id, const std::string& name,
    MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize);

  /**
     Collect a buffer, which will be combined with all
     other buffer during teardown.

     \param buffer a buffer to combine
   */
  void run(KVPairBuffer* buffer);

  /**
     Combine all buffer and then emit.
   */
  void teardown();
private:
  uint64_t coalescedBufferSize;
  std::queue<KVPairBuffer*> buffers;

  KVPairBufferFactory bufferFactory;
};

#endif // MAPRED_BUFFER_COMBINER_H
