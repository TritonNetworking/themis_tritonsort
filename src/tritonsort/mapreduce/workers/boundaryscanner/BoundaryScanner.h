#ifndef MAPRED_BOUNDARY_SCANNER_H
#define MAPRED_BOUNDARY_SCANNER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/SimpleKVPairWriter.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"

/**
   BoundaryScanner is responsible for scanning sorted key buffers and picking
   partition boundaries off at regular intervals. It uses sample metadata to
   determine the exact byte boundaries to use for partitions. Since all nodes
   need to use the same number of partitions, there is some synchronization at
   the beginning while the first node the cluster, the coordinator, uses all the
   sample metadata to decide the number of partitions.

   BoundaryScanner uses arbitrarily sized input and output buffers, and can
   handle its input or output split across multiple buffers. However, the first
   input buffer must include sample metadata. Additionally, input and output
   buffers must contain whole tuples.
 */
class BoundaryScanner : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the id of the worker

     \param name the name of the worker

     \param phaseName the name of the phase

     \param memoryAllocator a memory allocator used for new buffers

     \param defaultBufferSize the default size of output buffers

     \param alignmentSize the memory alignment of output buffers

     \param params the global params object

     \param isCoordinatorNode if true, this node is the coordinator

     \param coordinatorNodeID the ID of the coordinator node
   */
  BoundaryScanner(
    uint64_t id, const std::string& name, const std::string& phaseName,
    MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
    uint64_t alignmentSize, const Params& params, uint64_t numNodes,
    bool isCoordinatorNode, uint64_t coordinatorNodeID);

  /// Destructor
  virtual ~BoundaryScanner();

  /**
     Scan input buffers until partition boundaries are found and then emit them.

     \param buffer a sorted key buffer
   */
  void run(KVPairBuffer* buffer);

  /// Emit leftover chunk buffer
  void teardown();

private:
  /// Used internally to get output chunks for partition buffers
  /**
     \param tupleSize the size of the tuple that is about to be written

     \return a new buffer to write to
   */
  KVPairBuffer* getOutputChunk(uint64_t tupleSize);

  const bool isCoordinatorNode;
  const uint64_t coordinatorNodeID;
  const Params& params;
  const std::string& phaseName;
  const uint64_t numNodes;

  uint64_t jobID;

  KVPairBufferFactory bufferFactory;
  SimpleKVPairWriter writer;

  PhaseZeroSampleMetadata* sampleMetadata;
  uint64_t numPartitions;

  uint64_t bytesPerPartition;
  uint64_t remainder;

  uint64_t numPartitionsPicked;
  uint64_t nextPartitionBytes;

  uint64_t bytesScanned;
  uint64_t tuplesScanned;
};

#endif // MAPRED_BOUNDARY_SCANNER_H
