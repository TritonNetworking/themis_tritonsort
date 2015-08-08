#ifndef TRITONSORT_MAPREDUCE_SORT_STRATEGY_INTERFACE_H
#define TRITONSORT_MAPREDUCE_SORT_STRATEGY_INTERFACE_H

#include "common/sort_constants.h"

class KVPairBuffer;

/**
   This is the interface that all sort strategies used by Sorter workers must
   implement.
 */
class SortStrategyInterface {
public:
  /// Destructor
  virtual ~SortStrategyInterface() {}

  /**
     Run the appropriate sort algorithm.

     \param inputBuffer the input buffer to be sorted

     \param outputBuffer the output buffer where sorted tuples should be copied
   */
  virtual void sort(KVPairBuffer* inputBuffer, KVPairBuffer* outputBuffer) = 0;


  /// Determine how much memory this sort strategy will need to sort a buffer
  /**
     \param buffer the buffer that is to be sorted

     \return the amount of scratch memory in bytes this sort strategy will need
     for sorting the buffer
   */
  virtual uint64_t getRequiredScratchBufferSize(KVPairBuffer* buffer) const = 0;


  /// Provide the sort strategy with a scratch buffer
  /**
     It is expected that the caller called getRequiredScratchBufferSize to
     determine how big of a scratch buffer to provide to the strategy. It is
     the caller's responsibility to garbage collect this scratch memory once
     the sort has completed.

     \param scratchBuffer the scratch buffer with which to provide the strategy
   */
  virtual void setScratchBuffer(uint8_t* scratchBuffer) = 0;

  /// Return the SortAlgorithm corresponding to this strategy.
  virtual SortAlgorithm getSortAlgorithmID() const = 0;
};

#endif // TRITONSORT_MAPREDUCE_SORT_STRATEGY_INTERFACE_H
