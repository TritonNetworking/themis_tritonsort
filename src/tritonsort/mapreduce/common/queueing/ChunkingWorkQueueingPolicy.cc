#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/queueing/ChunkingWorkQueueingPolicy.h"

ChunkingWorkQueueingPolicy::ChunkingWorkQueueingPolicy(
  uint64_t _numDisksPerWorker, uint64_t numWorkers, ChunkMap& _chunkMap)
  : WorkQueueingPolicy(numWorkers),
    numDisksPerWorker(_numDisksPerWorker),
    chunkMap(_chunkMap) {
}

uint64_t ChunkingWorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  KVPairBuffer* buffer = dynamic_cast<KVPairBuffer*>(workUnit);
  ABORT_IF(buffer == NULL,
           "Tried to enqueue some work unit that isn't a KVPairBuffer.");

  // Create a new chunk.
  uint64_t partitionID = buffer->getLogicalDiskID();
  uint64_t chunkID = chunkMap.addChunk(partitionID, buffer->getCurrentSize());
  buffer->setChunkID(chunkID);

  // Get the disk ID and emit.
  uint64_t diskID = chunkMap.getDiskID(partitionID, chunkID);
  uint64_t queueID = diskID / numDisksPerWorker;

  TRITONSORT_ASSERT(queueID < numQueues,
         "Computed queue %llu from disk %llu and num disks per worker %llu, "
         "but number of queues is %llu", queueID,
         diskID, numDisksPerWorker, numQueues);

  return queueID;
}
