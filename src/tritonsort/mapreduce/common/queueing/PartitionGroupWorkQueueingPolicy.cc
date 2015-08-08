#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/queueing/PartitionGroupWorkQueueingPolicy.h"

PartitionGroupWorkQueueingPolicy::PartitionGroupWorkQueueingPolicy(
  uint64_t _partitionGroupsPerNode, uint64_t numWorkers)
  : WorkQueueingPolicy(numWorkers),
    partitionGroupsPerNode(_partitionGroupsPerNode) {
}

uint64_t PartitionGroupWorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  // Interpret the work unit as a KVPairBuffer and compute the partition group
  // the buffer belongs to.
  KVPairBuffer* buffer = dynamic_cast<KVPairBuffer*>(workUnit);
  ASSERT(buffer != NULL,
         "Expected KVPairBuffer but got some other kind of work unit.");

  return buffer->getPartitionGroup() % partitionGroupsPerNode;
}
