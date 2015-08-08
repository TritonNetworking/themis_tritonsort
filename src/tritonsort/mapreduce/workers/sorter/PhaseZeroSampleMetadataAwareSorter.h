#ifndef MAPRED_PHASE_ZERO_SAMPLE_METADATA_AWARE_SORTER_H
#define MAPRED_PHASE_ZERO_SAMPLE_METADATA_AWARE_SORTER_H

#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/workers/sorter/Sorter.h"

/**
   A variant of the Sorter worker designed for phase zero. In addition to
   sorting its input buffer, it copies the input buffer's phase zero metadata
   to the output buffer. Finally, rather than emitting a single huge buffer that
   will overwhelm the coordinator node, it emits smaller chunks using code that
   is similar to the TupleDemux's bulk tuple copy.
 */
class PhaseZeroSampleMetadataAwareSorter : public Sorter {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the ID of this worker within its parent stage

     \param name the name of the worker's parent stage

     \param outputNodeID the node ID of the coordinator, to which the sorted
     buffer will be sent by a subsequent stage

     \param memoryAllocator a memory allocator that this stage will use to
     allocate memory for buffers and scratch space

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param minBufferSize the minimum size of a phase zero sorter output buffer

     \param sortStrategyFactory a factory that will be used to populate the
     list of sort strategies

     \param maxRadixSortScratchSize the maximum number of scratch bytes to be
     used by radix sort
   */
  PhaseZeroSampleMetadataAwareSorter(
    uint64_t id, const std::string& name, uint64_t outputNodeID,
    MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize,
    uint64_t minBufferSize, const SortStrategyFactory& sortStrategyFactory,
    uint64_t maxRadixSortScratchSize);

private:
  /**
     Copies metadata from the input buffer to the output buffer

     \sa Sorter::prepareToWriteToOutputBuffer
   */
  virtual void prepareToWriteToOutputBuffer(
    KVPairBuffer* outputBuffer, KVPairBuffer* inputBuffer);

  /**
     Make sure the output buffer metadata will be transmitted along with its
     data, and then split the buffer into smaller chunks so we don't overload
     the coordinator node.

     \sa Sorter::emitOutputBuffer
   */
  virtual void emitOutputBuffer(KVPairBuffer* outputBuffer);

  /**
     Callback for SimpleKVPairWriter to get an output chunk buffer.

     \param tupleSize the size of the tuple that is about to be written

     \return a KVPairBuffer to hold the tuple
   */
  KVPairBuffer* getOutputChunk(uint64_t tupleSize);

  /**
     Create a new SampleMetadataKVPairBuffer as the output buffer.

     \param memory the underlying memory region to use

     \param size the size of the buffer

     \return a new output buffer
   */
  virtual KVPairBuffer* newOutputBuffer(uint8_t* memory, uint64_t size);

  /**
     Compute the output buffer size taking into account the metadata size

     \param inputBuffer the input buffer whose size will determine the output
     buffer size

     \return the output buffer size
   */
  virtual uint64_t getOutputBufferSize(KVPairBuffer& inputBuffer);

  const uint64_t minBufferSize;
  // The node ID of the coordinator, to which the downstream sender stage will
  // be routing sorted output buffers
  const uint64_t outputNodeID;

  KVPairBufferFactory bufferFactory;

  std::set<uint64_t> jobIDs;
};

#endif // MAPRED_PHASE_ZERO_SAMPLE_METADATA_AWARE_SORTER_H
