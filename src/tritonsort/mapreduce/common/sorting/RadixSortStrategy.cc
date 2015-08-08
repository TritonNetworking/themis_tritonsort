#include "RadixSortStrategy.h"

RadixSortStrategy::RadixSortStrategy(bool useSecondaryKeys)
  : radixSort(useSecondaryKeys) {
}

void RadixSortStrategy::sort(KVPairBuffer* inputBuffer,
                             KVPairBuffer* outputBuffer) {
  // TODO(MC): We might as well just make RadixSort inherit from SortStrategy
  // and get rid of this trivial class.
  radixSort.sort(inputBuffer, outputBuffer);
}

uint64_t RadixSortStrategy::getRequiredScratchBufferSize(
  KVPairBuffer* buffer) const {
  return radixSort.getRequiredScratchBufferSize(buffer);
}

void RadixSortStrategy::setScratchBuffer(uint8_t* scratchBuffer) {
  radixSort.setScratchBuffer(scratchBuffer);
}

SortAlgorithm RadixSortStrategy::getSortAlgorithmID() const {
  return RADIX_SORT_MAPREDUCE;
}
