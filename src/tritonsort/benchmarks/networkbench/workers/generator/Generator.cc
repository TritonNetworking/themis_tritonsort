#include "benchmarks/networkbench/workers/generator/Generator.h"

Generator::Generator(
  uint64_t id, const std::string& name,
  MemoryAllocatorInterface& memoryAllocator, uint64_t _numBuffers,
  uint64_t _bufferSize, uint64_t _numPartitionGroups, bool _remote,
  uint64_t _partitionGroupsPerNode, uint64_t _firstLocalPartitionGroup)
  : SelfStartingWorker(id, name),
    numBuffers(_numBuffers),
    bufferSize(_bufferSize),
    numPartitionGroups(_numPartitionGroups),
    remote(_remote),
    partitionGroupsPerNode(_partitionGroupsPerNode),
    firstLocalPartitionGroup(_firstLocalPartitionGroup),
    bufferFactory(*this, memoryAllocator, _bufferSize, 0) {
}

void Generator::run() {
  uint64_t partitionGroup = 0;
  for (uint64_t i = 0; i < numBuffers; i++) {
    KVPairBuffer* buffer = bufferFactory.newInstance();
    if (remote) {
      // Only send data to remote peers.
      // First pick a random partition group from the remote groups.
      partitionGroup = lrand48() % (
        numPartitionGroups - partitionGroupsPerNode);
      // If this group belongs to a node that succeeds this one in the peer
      // list, we're going to need to shift the partition group ID over by 1
      // node to account for the fact that we're not sending to localhost.
      if (partitionGroup >= firstLocalPartitionGroup) {
        partitionGroup += partitionGroupsPerNode;
      }
    } else {
      // Send data uniformly to all peers including localhost.
      partitionGroup = lrand48() % (numPartitionGroups);
    }
    buffer->setPartitionGroup(partitionGroup);
    buffer->setCurrentSize(buffer->getCapacity());
    buffer->addJobID(0);
    emitWorkUnit(buffer);
  }
}

BaseWorker* Generator::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t dataSize = params.getv<uint64_t>(
    "BENCHMARK_DATA_SIZE_PER_NODE.%s", phaseName.c_str());
  uint64_t bufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s", phaseName.c_str(), stageName.c_str());
  uint64_t numWorkers = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t numBuffers = dataSize / (bufferSize * numWorkers);
  uint64_t numPartitionGroups = params.get<uint64_t>("NUM_PARTITION_GROUPS");

  uint64_t partitionGroupsPerNode = params.get<uint64_t>(
    "PARTITION_GROUPS_PER_NODE");
  uint64_t localPeerID = params.get<uint64_t>("MYPEERID");
  uint64_t firstLocalPartitionGroup = partitionGroupsPerNode * localPeerID;
  bool remote = params.get<bool>("REMOTE_CONNECTIONS_ONLY");

  Generator* generator = new Generator(
    id, stageName, memoryAllocator, numBuffers, bufferSize, numPartitionGroups,
    remote, partitionGroupsPerNode, firstLocalPartitionGroup);

  return generator;
}
