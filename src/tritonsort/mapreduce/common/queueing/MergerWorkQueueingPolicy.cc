#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/queueing/MergerWorkQueueingPolicy.h"

MergerWorkQueueingPolicy::MergerWorkQueueingPolicy(
  uint64_t _totalNumChunks, ChunkMap& chunkMap)
  : WorkQueueingPolicy(_totalNumChunks),
    totalNumChunks(_totalNumChunks) {
  const ChunkMap::DiskMap& diskMap = chunkMap.getDiskMap();

  // Build the offset map.
  uint64_t currentOffset = 0;
  for (ChunkMap::DiskMap::const_iterator iter = diskMap.begin();
       iter != diskMap.end(); iter++) {
    uint64_t partitionID = iter->first;
    uint64_t numChunks = (iter->second).size();

    offsetMap[partitionID] = currentOffset;
    currentOffset += numChunks;
  }
}

uint64_t MergerWorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  KVPairBuffer* buffer = dynamic_cast<KVPairBuffer*>(workUnit);
  ABORT_IF(buffer == NULL,
           "Tried to enqueue some work unit that isn't a KVPairBuffer.");

  uint64_t partitionID = buffer->getLogicalDiskID();
  uint64_t chunkID = buffer->getChunkID();

  // Compute queueID from chunk and offset.
  uint64_t queueID = offsetMap.at(partitionID) + chunkID;

  TRITONSORT_ASSERT(queueID < totalNumChunks,
         "Computed queue %llu from partition ID %llu and chunk ID %llu, "
         "but number of queues is %llu", queueID, partitionID, chunkID,
         totalNumChunks);

  return queueID;
}
