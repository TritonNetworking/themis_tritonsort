#ifndef MAPRED_MERGER_WORK_QUEUEING_POLICY_H
#define MAPRED_MERGER_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"
#include "mapreduce/common/ChunkMap.h"

/**
   MergerWorkQueueingPolicy is a queueing policy specifically built
   for the Merger stage in phase three. Each partition chunk is assigned
   to its own queue, so that the Merger can merge chunks from all partitions
   simultaneously.
 */
class MergerWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param totalNumChunks the total number of chunks we're merging

     \param chunkMap the global chunk map
   */
  MergerWorkQueueingPolicy(
    uint64_t totalNumChunks, ChunkMap& chunkMap);

private:
  /**

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  const uint64_t totalNumChunks;

  std::map<uint64_t, uint64_t> offsetMap;

  DISALLOW_COPY_AND_ASSIGN(MergerWorkQueueingPolicy);
};

#endif // MAPRED_MERGER_WORK_QUEUEING_POLICY_H
