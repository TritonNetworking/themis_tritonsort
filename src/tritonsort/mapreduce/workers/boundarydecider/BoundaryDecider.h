#ifndef MAPRED_BOUNDARY_DECIDER_H
#define MAPRED_BOUNDARY_DECIDER_H

#include <vector>

#include "core/MultiQueueRunnable.h"
#include "core/constants.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/SimpleKVPairWriter.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/QuickSortStrategy.h"

/**
   BoundaryDecider is responsible for deciding upon the job-wide partition
   boundary list. Each node picks its own version of the boundary list in the
   BoundaryScanner. Those lists are funneled into a single BoundaryDecider, which
   chooses the median key for each partition as the official boundary key.
 */
class BoundaryDecider : public MultiQueueRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the id of the worker

     \param name the name of the worker

     \param memoryAllocator a memory allocator used for new buffers

     \param defaultBufferSize the default size of output buffers

     \param alignmentSize the memory alignment of output buffers

     \param numNodes the number of nodes in the cluster
   */
  BoundaryDecider(
    uint64_t id, const std::string& stageName,
    MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
    uint64_t alignmentSize, uint64_t numNodes);

  /**
     Take in boundary buffers from all nodes and, for each partition, sort the
     keys and pick the median key as the official boundary key.
   */
  void run();

private:
  typedef std::vector<KVPairBuffer*> BufferVector;
  typedef std::vector<KeyValuePair> TupleVector;

  /// Used internally to get an output chunk buffer
  /**
     \param tupleSize the size of a tuple to be written

     \return a new chunk buffer
   */
  KVPairBuffer* getOutputChunk(uint64_t tupleSize);

  /// Used internally to broadcast chunk buffers to all nodes.
  /**
     \param outputChunk the chunk to broadcast
   */
  void broadcastOutputChunk(KVPairBuffer* outputChunk);

  const uint64_t numNodes;

  uint64_t jobID;

  KVPairBufferFactory bufferFactory;
  SimpleKVPairWriter writer;

  BufferVector buffers;
  TupleVector kvPairs;

  QuickSortStrategy sortStrategy;
};

#endif // MAPRED_BOUNDARY_DECIDER_H
