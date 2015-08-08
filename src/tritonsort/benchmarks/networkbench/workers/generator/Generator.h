#ifndef NETWORKBENCH_GENERATOR_H
#define NETWORKBENCH_GENERATOR_H

#include "core/SelfStartingWorker.h"
#include "mapreduce/common/KVPairBufferFactory.h"

/**
   Generator is a worker that generates sender buffers with random partition
   IDs to simulate buffers arriving randomly from the mappers.
 */
class Generator : public SelfStartingWorker {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param name the stage name

     \param memoryAllocator an allocator to use when creating new buffers

     \param numBuffers the total number of buffers this generator should
     create

     \param bufferSize the size of sender buffers

     \param numPartitionGroups the number of partition groups in the cluster

     \param remote if true only send to remote connections

     \param partitionGroupsPerNode the number of partition groups per node

     \param firstLocalPartitionGroup the ID of the first partition group local
     to this node
   */
  Generator(
    uint64_t id, const std::string& name,
    MemoryAllocatorInterface& memoryAllocator, uint64_t numBuffers,
    uint64_t bufferSize, uint64_t numPartitionGroups, bool remote,
    uint64_t partitionGroupsPerNode, uint64_t firstLocalPartitionGroup);

  /// Destructor
  virtual ~Generator() {}

  /// Generate buffers to be sent over the network
  void run();

private:
  const uint64_t numBuffers;
  const uint64_t bufferSize;
  const uint64_t numPartitionGroups;
  const bool remote;
  const uint64_t partitionGroupsPerNode;
  const uint64_t firstLocalPartitionGroup;
  KVPairBufferFactory bufferFactory;
};

#endif // NETWORKBENCH_GENERATOR_H
