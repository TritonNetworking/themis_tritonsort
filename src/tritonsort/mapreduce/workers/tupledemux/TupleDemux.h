#ifndef MAPRED_TUPLE_DEMUX_H
#define MAPRED_TUPLE_DEMUX_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/ListableKVPairBufferFactory.h"
#include "mapreduce/common/PartitionMap.h"

class KVPairBuffer;
class ListableKVPairBuffer;
class Params;
class PartialKVPairWriter;
class PartitionFunctionInterface;
class PartitionFunctionMap;

/**
   The tuple demux is responsible for demultiplexing tuples from incoming
   buffers into one of a number of small logical disk buffers, one per
   partition.
 */
class TupleDemux : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \todo (AR) some of these parameters can be derived from the others; we
     should probably remove redundant parameters at some point

     \param id the worker ID for this worker

     \param name the name of the stage for which the worker is working

     \param nodeID the node ID of the node on which this worker is executing

     \param partitionFunctionMap the mapping from jobs to partition functions

     \param memoryAllocator the allocator to use for creating new buffers

     \param defaultBufferSize the default size of a buffer allocated by this
     worker

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param serializeWithoutHeaders if true then the demux will write tuples
     without headers

     \param deserializeWithoutHeaders if true then the demux will read tuples
     without headers

     \param numDemuxes the number of Tuple Demux workers per node

     \param params the global params object

     \param phaseName the name of the phase

     \param minutesort true if we're using daytona minutesort
   */
  TupleDemux(
    uint64_t id, const std::string& name, uint64_t nodeID,
    PartitionFunctionMap& partitionFunctionMap,
    MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
    uint64_t alignmentSize, bool serializeWithoutHeaders,
    bool deserializeWithoutHeaders, uint32_t fixedKeyLength,
    uint32_t fixedValueLength, uint64_t numDemuxes, const Params& params,
    const std::string& phaseName, bool minutesort);

  void run(KVPairBuffer* buffer);

  /**
     If any logical disk buffers that are partially full remain, sends those
     buffers to the next stage. If any logical disk buffers remain in the
     buffer queue, returns those buffers to the buffer pool.
   */
  void teardown();

private:
  KVPairBuffer* newBuffer();

  void emitBuffer(KVPairBuffer* buffer, uint64_t unused);

  /// The node ID of the node on which the worker is executing
  const uint64_t nodeID;

  /// The number of tuple demux workers per node
  const uint64_t numDemuxes;

  /// Whether we're writing tuples without headers or not
  const bool serializeWithoutHeaders;

  /// Whether we're reading tuples without headers or not
  const bool deserializeWithoutHeaders;

  /// If non-zero, read tuples with a fixed key length.
  const uint32_t fixedKeyLength;

  /// If non-zero, read tuples with a fixed value length.
  const uint32_t fixedValueLength;

  /// If true, then check for large partitions in minutesort.
  const bool minutesort;

  ListableKVPairBufferFactory listableBufferFactory;

  StatLogger logger;
  uint64_t ldBufferGets;

  PartitionFunctionMap& partitionFunctionMap;

  PartitionFunctionInterface* partitionFunction;

  // Demux can write partial tuples since all downstream stages only care about
  // bytes and not tuples.
  PartialKVPairWriter* writer;

  PartitionMap partitionMap;

  // State for the current buffer.
  uint64_t partitionGroup;

  // State that depends on the job.
  uint64_t jobID;
  uint64_t partitionsPerGroup;
  uint64_t partitionOffset;

  bool flushedWriter;
  std::set<uint64_t> largePartitions;
};

#endif // MAPRED_TUPLE_DEMUX_H
