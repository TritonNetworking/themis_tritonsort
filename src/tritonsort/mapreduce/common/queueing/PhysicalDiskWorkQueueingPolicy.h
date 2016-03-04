#ifndef MAPRED_PHYSICAL_DISK_WORK_QUEUEING_POLICY_H
#define MAPRED_PHYSICAL_DISK_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"
#include "mapreduce/common/PartitionMap.h"

class Params;

/**
   PhysicalDiskWorkQueueingPolicy is a tracker queuing policy specifically
   designed for the Writer and Chainer stages. Since each of these workers is
   responsible for a particular set of disks, work units must be issued to the
   queue that handles buffers destined for the appropriate disk.

   This queueing policy automatically inspects the logical disk ID of a
   worker buffer and assigns it the appropriate worker queue.
 */
class PhysicalDiskWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param numDisksPerWorker the number of physical disks each worker is
     responsible for

     \param the number of workers in the tracker

     \param params the global params object

     \param phaseName the name of the phase
   */
  PhysicalDiskWorkQueueingPolicy(
    uint64_t numDisksPerWorker, uint64_t numWorkers, const Params& params,
    const std::string& phaseName);

  /**
     Compute the disk that a KVPairBuffer or
     BufferListContainer<ListableKVPairBuffer> corresponds to.

     \param workUnit a KVPairBuffer or BufferListContainer<ListableKVPairBuffer>

     \param partitionMap a PartitionMap containing partition data for the
     current job

     \param numDisksPerNode the number of disks on each node

     \return the ID of the disk that the resource is destined for.
   */
  static uint64_t computeDisk(
    Resource* workUnit, PartitionMap& partitionMap, uint64_t numDisksPerNode);

private:
  /**
     Automatically inspects the logical disk ID of buffers and assigns them to
     the queue corresponding to the worker that handles the destination disk.

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  const uint64_t numDisksPerWorker;
  const uint64_t numDisksPerNode;

  PartitionMap partitionMap;
};

#endif // MAPRED_PHYSICAL_DISK_WORK_QUEUEING_POLICY_H
