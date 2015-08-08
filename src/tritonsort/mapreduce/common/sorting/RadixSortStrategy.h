#ifndef TRITONSORT_MAPREDUCE_RADIX_SORT_STRATEGY_H
#define TRITONSORT_MAPREDUCE_RADIX_SORT_STRATEGY_H

#include "radixsort/RadixSort.h"
#include "SortStrategyInterface.h"

/**
   A sort strategy implementation that uses a distribution-based radix sort to
   sort tuples by key. Buffers of keys and variably sized tuple offsets are
   constructed from scratch space provided by an external source (typically a
   Sorter worker).

   RadixSortStrategy's performance is highly variable depending on the input
   data. It has been clockd as fast as 205 MBps on graysort input data. However,
   variance in key size can dramatically reduce performance because the runtime
   and scratch buffer overhead are roughly linear in the maximum key size.
   Having even one key that is significantly larger than the others can greatly
   reduce throughput.

   RadixSortStrategy is a wrapper around a RadixSort object that performs the
   actual sort. We should remove this wrapper class and make RadixSort a sort
   strategy.
 */
class RadixSortStrategy : public SortStrategyInterface {
public:
  /// Constructor
  RadixSortStrategy(bool useSecondaryKeys);

  /**
     Execute the radix sort.

     \sa SortStrategyInterface::sort
   */
  void sort(KVPairBuffer* inputBuffer, KVPairBuffer* outputBuffer);

  /**
     Radix sort's memory requirements are based on the maximum key size and the
     length of the buffer itself.

     \sa SortStrategyInterface::getRequiredScratchBufferSize
   */
  uint64_t getRequiredScratchBufferSize(KVPairBuffer* buffer) const;

  /// \sa RadixSortStrategyInterface::setScratchBuffer
  void setScratchBuffer(uint8_t* scratchBuffer);

  /// \sa SortStrategyInterface::getSortAlgorithmID
  SortAlgorithm getSortAlgorithmID() const;
private:
  RadixSort radixSort;
};

#endif //TRITONSORT_MAPREDUCE_RADIX_SORT_STRATEGY_H
