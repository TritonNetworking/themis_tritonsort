#ifndef MAPRED_BOUNDARY_DESERIALIZER_H
#define MAPRED_BOUNDARY_DESERIALIZER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/PartitionMap.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

class Params;

/**
   Given a collection of buffers that represents the boundary list, construct an
   actual KeyPartitionerInterface object that can be used for partitioning.
 */
class BoundaryDeserializer : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker's worker ID within its parent stage

     \param name the worker's stage name

     \param localNodeID the ID of this node

     \param memoryAllocator the memory allocator to use for constructing buffers

     \param numNodes the number of nodes in the cluster

     \param numPartitionGroups the number of partition groups in the cluster

     \param params the global params object

     \param phaseName the name of the phase

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size
   */
  BoundaryDeserializer(
    uint64_t id, const std::string& name, uint64_t localNodeID,
    MemoryAllocatorInterface& memoryAllocator, uint64_t numNodes,
    uint64_t numPartitionGroups, const Params& params,
    const std::string& phaseName, uint64_t alignmentSize);

  /**
     Collect a fragment of the boundary list, which will be combined with all
     other fragments during teardown.

     \param buffer a fragment of the boundary list to deserialize
   */
  void run(KVPairBuffer* buffer);

  /**
     Combine all fragments of the boundary list, and then construct an actual
     KeyPartitionerInterface object, which is emitted.
   */
  void teardown();
private:
  const uint64_t localNodeID;
  const uint64_t numNodes;
  const uint64_t numPartitionGroups;

  uint64_t coalescedBufferSize;
  typedef std::vector<std::queue<KVPairBuffer*> > BufferVector;
  BufferVector buffers;

  KVPairBufferFactory bufferFactory;

  PartitionMap partitionMap;
};

#endif // MAPRED_BOUNDARY_DESERIALIZER_H
