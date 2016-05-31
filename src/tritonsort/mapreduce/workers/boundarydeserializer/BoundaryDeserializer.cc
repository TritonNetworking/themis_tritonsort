#include <limits>

#include "mapreduce/common/PhaseZeroOutputData.h"
#include "mapreduce/common/Utils.h"
#include "mapreduce/common/boundary/KeyPartitioner.h"
#include "mapreduce/workers/boundarydeserializer/BoundaryDeserializer.h"

BoundaryDeserializer::BoundaryDeserializer(
  uint64_t id, const std::string& name, uint64_t _localNodeID,
  MemoryAllocatorInterface& memoryAllocator, uint64_t _numNodes,
  uint64_t _numPartitionGroups, const Params& params,
  const std::string& phaseName, uint64_t alignmentSize)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    localNodeID(_localNodeID),
    numNodes(_numNodes),
    numPartitionGroups(_numPartitionGroups),
    coalescedBufferSize(0),
    bufferFactory(*this, memoryAllocator, 0, alignmentSize),
    partitionMap(params, phaseName) {
}

void BoundaryDeserializer::run(KVPairBuffer* buffer) {
  coalescedBufferSize += buffer->getCurrentSize();
  buffers.push(buffer);
}

void BoundaryDeserializer::teardown() {
  // Coalesce all buffers
  KVPairBuffer* buffer = bufferFactory.newInstance(coalescedBufferSize);

  uint64_t jobID = std::numeric_limits<uint64_t>::max();
  while (!buffers.empty()) {
    KVPairBuffer* nextBuffer = buffers.front();
    buffers.pop();

    if (jobID == std::numeric_limits<uint64_t>::max()) {
      const std::set<uint64_t>& jobIDs = nextBuffer->getJobIDs();

      ASSERT(jobIDs.size() == 1, "Expected the first buffer entering the "
             "deserializer to have exactly one job ID; this one has %llu",
             jobIDs.size());
      jobID = *(jobIDs.begin());
    }

    const uint8_t* appendPtr = buffer->setupAppend(
      nextBuffer->getCurrentSize());
    memcpy(
      const_cast<uint8_t*>(appendPtr), nextBuffer->getRawBuffer(),
      nextBuffer->getCurrentSize());
    buffer->commitAppend(appendPtr, nextBuffer->getCurrentSize());

    delete nextBuffer;
  }

  uint64_t numPartitions = partitionMap.getNumPartitions(jobID);

  // Construct a key partitioner for this node.
  KeyPartitionerInterface* keyPartitioner = new KeyPartitioner(
    *buffer, localNodeID, numNodes, numPartitionGroups, numPartitions);

  // We no longer need the buffer
  delete buffer;

  PhaseZeroOutputData* outputData = new PhaseZeroOutputData(keyPartitioner);

  // Send the boundary list to the next stage
  emitWorkUnit(outputData);
}

BaseWorker* BoundaryDeserializer::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t localNodeID = params.get<uint64_t>("MYPEERID");

  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");

  uint64_t numPartitionGroups = params.get<uint64_t>("NUM_PARTITION_GROUPS");

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  BoundaryDeserializer* deserializer = new BoundaryDeserializer(
    id, stageName, localNodeID, memoryAllocator, numNodes,
    numPartitionGroups, params, phaseName, alignmentSize);

  return deserializer;
}
