#ifndef MAPRED_REDUCER_H
#define MAPRED_REDUCER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/PartitionMap.h"

class KVPairBuffer;
class KVPairWriter;
class KVPairWriterInterface;
class ReduceFunction;

/**
   Worker that applies a reduce function to groups of tuples in a buffer with
   the same key

   \TODO: Phase three will use more than one buffer per partition as input to
   the Reducer. We should really have a special case iterator for this so key
   groups can span multiple buffers.
 */
class Reducer : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL
public:
  /// Constructor
  /**
     \param id the worker ID of this worker within its parent stage

     \param name the name of the stage for which this worker is working

     \param nodeID the node ID for the node on which this worker is working

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param memoryAllocator a memory allocator that this worker will use to
     allocate memory for buffers

     \param defaultBufferSize the default size of the buffers that this worker
     will emit

     \param coordinatorClient client that the reducer will use to get job
     information from the coordinator

     \param params a Params object that will be used to configure the reduce
     function

     \param outputReplicationLevel if greater than one, reducer output buffers
     will be replicated to remote writer replica pipeline

     \param phaseName the name of the phase

     \param numNodes the number of nodes in the cluster
   */
  Reducer(
    uint64_t id, const std::string& name, uint64_t nodeID,
    uint64_t alignmentSize, MemoryAllocatorInterface& memoryAllocator,
    uint64_t defaultBufferSize, CoordinatorClientInterface& coordinatorClient,
    const Params& params, uint64_t outputReplicationLevel,
    const std::string& phaseName, uint64_t numNodes);

  /// Destructor
  virtual ~Reducer();

  /// Iterate through the buffer, applying the reduce function to each
  /// contiguous group of tuples with the same key
  /**
     This method presupposes that the buffer is sorted.

     \param buffer the buffer over which to iterate and apply the reduce
     function
   */
  void run(KVPairBuffer* buffer);

  /// Flush all partially-full buffers and log statistics
  void teardown();

  void setWriter(KVPairWriterInterface* writer);
private:
  KVPairBuffer* newBuffer();

  void emitBuffer(KVPairBuffer* buffer, uint64_t unused);

  // The node ID of the node on which this reducer is running
  const uint64_t nodeID;

  const Params& params;

  const uint64_t outputReplicationLevel;
  const uint64_t numNodes;

  KVPairWriterInterface* writer;

  ReduceFunction* reduceFunction;

  KVPairBufferFactory bufferFactory;

  uint64_t bytesIn;
  uint64_t bytesOut;

  StatLogger logger;

  // Logical disk UID to be assigned to buffers retrieved by the KVPairWriter
  uint64_t logicalDiskUID;

  uint64_t jobID;


  PartitionMap partitionMap;

  CoordinatorClientInterface& coordinatorClient;
};

#endif // MAPRED_REDUCER_H
