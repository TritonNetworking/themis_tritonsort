#ifndef MAPRED_NETWORK_DESTINATION_WORK_QUEUEING_POLICY_H
#define MAPRED_NETWORK_DESTINATION_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"

/**
   NetworkDestinationWorkQueueingPolicy is a tracker queuing policy specifically
   designed for the Sender stage. The sender uses one queue per destination, so
   buffers must be placed in the correct queue or they will be sent to the wrong
   nodes.

   This policy uses the node field in a KVPairBuffer as indication of the
   buffer's destination unless the partitionGroup field is also set. In this
   case, the node field is assumed to be garbage and must be recomputed based
   on the partitionGroup field and the placement of partitions in the cluster.
 */
class NetworkDestinationWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param partitionGroupsPerNode the number of partition groups per node

     \param numPeers the number of peers that we are sending buffers to
   */
  NetworkDestinationWorkQueueingPolicy(
    uint64_t partitionGroupsPerNode, uint64_t numPeers);

private:
  /**
     Automatically inspects the node and partitionGroup fields of buffers and
     assigns them to the queue corresponding to the destination node for the
     buffer.

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  const uint64_t partitionGroupsPerNode;

  DISALLOW_COPY_AND_ASSIGN(NetworkDestinationWorkQueueingPolicy);
};

#endif // MAPRED_NETWORK_DESTINATION_WORK_QUEUEING_POLICY_H
