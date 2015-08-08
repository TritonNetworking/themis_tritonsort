#include "common/BufferListContainer.h"
#include "core/Params.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/buffers/ListableKVPairBuffer.h"
#include "mapreduce/common/queueing/PhysicalDiskWorkQueueingPolicy.h"

PhysicalDiskWorkQueueingPolicy::PhysicalDiskWorkQueueingPolicy(
  uint64_t _numDisksPerWorker, uint64_t numWorkers, const Params& params,
  const std::string& phaseName)
  : WorkQueueingPolicy(numWorkers),
    numDisksPerWorker(_numDisksPerWorker),
    numDisksPerNode(_numDisksPerWorker * numWorkers),
    partitionMap(params, phaseName) {
}

uint64_t PhysicalDiskWorkQueueingPolicy::computeDisk(
  Resource* workUnit, PartitionMap& partitionMap, uint64_t numDisksPerNode) {
  uint64_t physicalDisk = 0;
  KVPairBuffer* buffer = dynamic_cast<KVPairBuffer*>(workUnit);
  if (buffer != NULL) {
    // As expected this is a KVPairBuffer.
    const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
    ASSERT(jobIDs.size() == 1, "Expected one job ID, but got %llu",
           jobIDs.size());
    uint64_t jobID = *(jobIDs.begin());

    // Get the ID of the disk on this node.
    physicalDisk = (buffer->getLogicalDiskID() /
                    partitionMap.getNumPartitionsPerOutputDisk(jobID)) %
      numDisksPerNode;
  } else {
    // This might be a list of buffers passed by the chainer.
    BufferListContainer<ListableKVPairBuffer>* container =
      dynamic_cast<BufferListContainer<ListableKVPairBuffer>*>(workUnit);
    ASSERT(container != NULL,
           "Expected KVPairBuffer or BufferListContainer<KVPairBuffer> but got "
           "some other kind of work unit.");

    physicalDisk = container->getDiskID();
  }

  return physicalDisk;
}

uint64_t PhysicalDiskWorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  uint64_t physicalDisk = computeDisk(workUnit, partitionMap, numDisksPerNode);

  // Determine the worker responsible for this disk.
  ASSERT(physicalDisk / numDisksPerWorker < numQueues,
         "Computed queue %llu from disk %llu and num disks per worker %llu, "
         "but number of queues is %llu", physicalDisk / numDisksPerWorker,
         physicalDisk, numDisksPerWorker, numQueues);
  return physicalDisk / numDisksPerWorker;
}
