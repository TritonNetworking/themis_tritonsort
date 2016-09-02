#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/queueing/NetworkDestinationWorkQueueingPolicy.h"

NetworkDestinationWorkQueueingPolicy::NetworkDestinationWorkQueueingPolicy(
  uint64_t _partitionGroupsPerNode, uint64_t numPeers)
  : WorkQueueingPolicy(numPeers),
    partitionGroupsPerNode(_partitionGroupsPerNode) {
}

uint64_t NetworkDestinationWorkQueueingPolicy::getEnqueueID(
  Resource* workUnit) {
  // Interpret the work unit as a KVPairBuffer and compute the destination
  // queue.
  KVPairBuffer* buffer = dynamic_cast<KVPairBuffer*>(workUnit);
  TRITONSORT_ASSERT(buffer != NULL,
         "Expected KVPairBuffer but got some other kind of work unit.");

  uint64_t partitionGroup = buffer->getPartitionGroup();
  if (partitionGroup != std::numeric_limits<uint64_t>::max()) {
    // Compute the actual node ID from the partition group.
    uint64_t nodeID = partitionGroup / partitionGroupsPerNode;
    buffer->setNode(nodeID);

    return nodeID;
  } else {
    // The buffer's node field should already be set properly if partitionGroup
    // isn't specified.
    return buffer->getNode();
  }
}
