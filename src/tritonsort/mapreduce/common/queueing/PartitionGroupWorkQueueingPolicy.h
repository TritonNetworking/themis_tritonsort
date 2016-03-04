#ifndef MAPRED_PARTITION_GROUP_WORK_QUEUEING_POLICY_H
#define MAPRED_PARTITION_GROUP_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"

/**
   Routes buffers to workers based on the partition group field of the buffer.
   Used to route to the appropriate demux.
 */
class PartitionGroupWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param partitionGroupsPerNode the number of partition groups per node

     \param numWorkers the number of workers in the tracker
   */
  PartitionGroupWorkQueueingPolicy(
    uint64_t partitionGroupsPerNode, uint64_t numWorkers);

private:
  /**
     Automatically inspects the partition group of buffers and assigns them to
     the corresponding queue.

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  const uint64_t partitionGroupsPerNode;
};

#endif // MAPRED_PARTITION_GROUP_WORK_QUEUEING_POLICY_H
