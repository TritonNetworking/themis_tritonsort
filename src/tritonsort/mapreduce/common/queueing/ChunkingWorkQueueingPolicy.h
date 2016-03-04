#ifndef MAPRED_CHUNKING_WORK_QUEUEING_POLICY_H
#define MAPRED_CHUNKING_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"
#include "mapreduce/common/ChunkMap.h"

/**
   ChunkingWorkQueueingPolicy is a work queueing policy that assigns sorted
   partition chunk buffers to writers in phase 3. It uses the ChunkMap data
   structure to assign chunk IDs and pick the appropriate writer for the chunk.
 */
class ChunkingWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param numDisksPerWorker the number of physical disks each worker is
     responsible for

     \param the number of workers in the tracker

     \param chunkMap the global chunk map
   */
  ChunkingWorkQueueingPolicy(
    uint64_t numDisksPerWorker, uint64_t numWorkers, ChunkMap& chunkMap);

private:
  /**
     Assigns increasing chunk IDs to buffers and emits them to writer disks
     in a round-robin fashion.

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  const uint64_t numDisksPerWorker;

  ChunkMap& chunkMap;
};

#endif // MAPRED_CHUNKING_WORK_QUEUEING_POLICY_H
