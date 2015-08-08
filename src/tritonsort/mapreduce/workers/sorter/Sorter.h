#ifndef MAPRED_SORTER_H
#define MAPRED_SORTER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"
#include "mapreduce/common/sorting/SortStrategyInterface.h"

class RawBuffer;

/**
   The Sorter worker is responsible copying the sorted permutation of an
   input buffer's data into an output buffer.
 */
class Sorter : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the ID of this worker within its parent stage

     \param name the name of the worker's parent stage

     \param memoryAllocator a memory allocator that this stage will use to
     allocate memory for buffers and scratch space

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param sortStrategyFactory a factory that will be used to populate the
     list of sort strategies

     \param maxRadixSortScratchSize the maximum number of scratch bytes to be
     used by radix sort
   */
  Sorter(
    uint64_t id, const std::string& name,
    MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize,
    const SortStrategyFactory& sortStrategyFactory,
    uint64_t maxRadixSortScratchSize);

  /// Sort the given buffer, storing the buffer's sorted tuples in an output
  /// buffer
  /**
     \param inputBuffer the buffer whose contents are to be sorted
   */
  void run(KVPairBuffer* inputBuffer);

  /// Log statistics about the number of tuples and bytes input and output, and
  /// garbage collect the sort strategy passed in the constructor
  void teardown();

protected:
  const uint64_t alignmentSize;

  MemoryAllocatorInterface& memoryAllocator;

private:
  typedef std::vector<SortStrategyInterface*> SortStrategyInterfaceList;

  /**
     Perform any cleanup or manipulation on the output buffer so that it is
     prepared to accept the sorted permutation of the input buffer.

     \param outputBuffer the buffer to be prepared

     \param inputBuffer the buffer whose sorted data will be written to
     outputBuffer
   */
  virtual void prepareToWriteToOutputBuffer(
    KVPairBuffer* outputBuffer, KVPairBuffer* inputBuffer);


  /**
     Emit the sorted output buffer to the next stage.

     \param outputBuffer the buffer to emit
   */
  virtual void emitOutputBuffer(KVPairBuffer* outputBuffer);

  /**
     Create a new KVPairBuffer as the output buffer.

     \param memory the underlying memory region to use

     \param size the size of the buffer

     \return a new output buffer
   */
  virtual KVPairBuffer* newOutputBuffer(uint8_t* memory, uint64_t size);

  /**
     Compute the output buffer size.

     \param inputBuffer the input buffer whose size will determine the output
     buffer size

     \return the output buffer size
   */
  virtual uint64_t getOutputBufferSize(KVPairBuffer& inputBuffer);

  uint64_t scratchMemoryCallerID;

  // A list of sortStrategies populated by
  // SortStrategyFactory::populateOrderedSortStrategyList during construction
  SortStrategyInterfaceList sortStrategies;

  // Radix sort specific data:
  uint64_t maxRadixSortScratchSize;

  uint64_t sortTimeStatID;
  uint64_t numTuplesStatID;
  uint64_t tuplesInStatID;
  uint64_t tuplesOutStatID;
  uint64_t bytesInStatID;
  uint64_t bytesOutStatID;
  uint64_t sortAlgorithmUsedStatID;

  uint64_t numWorkUnitsSorted, numTuplesIn, numTuplesOut,
    totalBytesIn, totalBytesOut;

  StatLogger logger;
  Timer sortTimer;
};

#endif // MAPRED_SORTER_H
