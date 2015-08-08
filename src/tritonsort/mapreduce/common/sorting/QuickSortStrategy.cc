#include "core/TritonSortAssert.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/QuickSortStrategy.h"

QuickSortStrategy::QuickSortStrategy(bool useSecondaryKeys)
  : logger("QuickSort") {

  if (useSecondaryKeys) {
    comparisonFunction = compareTagsWithSecondaryKey;
  } else {
    comparisonFunction = compareTags;
  }

  sortTimeStatID = logger.registerStat("sort_time");
  populateTimeStatID = logger.registerStat("populate_time");
  qsortTimeStatID = logger.registerStat("qsort_time");
  collectTimeStatID = logger.registerStat("collect_time");

  scratchBuffer = NULL;
}

void QuickSortStrategy::sort(KVPairBuffer* inputBuffer,
                             KVPairBuffer* outputBuffer) {
  // ===General Strategy===
  // This strategy performs a C qsort() on an array of tags. Tags are pointers
  // to tuples in the input buffer.  Key lengths are read from the tuple headers
  // using these pointers. KeyValuePair::compare() is used to
  // perform the key comparisons.  This function name is a little misleading
  // since it operates on arbitrary byte streams, so it works without
  // requiring heavyweight KeyValuePair objects.
  //
  // This strategy replaces the previous strategy that included key lengths in
  // the tags. Reading key lengths from tuples increases speed by roughly
  // 10 MBps.
  //
  // A further optimization was made to the tag population routine so that it
  // reads tuples manually rather than using the getNextKVPair() method of
  // KVPairBuffer. This optmization increases speed by yet another 10 MBps.
  //
  // As of 11/07/11 it does not appear that there is much else that can be done
  // to optimize this strategy as long as it continues to use C qsort.
  // ======================
  ABORT_IF(inputBuffer == NULL, "Must set non-NULL input buffer.");
  ABORT_IF(outputBuffer == NULL, "Must set non-NULL output buffer.");

  ASSERT(inputBuffer->getCurrentSize() <= outputBuffer->getCapacity(), "Output "
         "buffer (capacity %llu) must be at least as large as input buffer "
         "(size %llu) to sort.", outputBuffer->getCapacity(),
         inputBuffer->getCurrentSize());

  Timer sortTimer;
  sortTimer.start();

  Timer timer;
  timer.start();

  uint64_t numTuples = inputBuffer->getNumTuples();

  // Check for presence of a scratch buffer
  ABORT_IF(scratchBuffer == NULL, "Scratch buffer wasn't set prior to sorting");

  // Interpret the scratch buffer as an array of uint8_t* tags for the purpose
  // of quick sort.
  uint8_t** tags = reinterpret_cast<uint8_t**>(scratchBuffer);

  // Populate tags.
  // Iterate over the buffer manually for extra speed.
  uint8_t* buffer = const_cast<uint8_t*>(inputBuffer->getRawBuffer());
  uint8_t* end = buffer + inputBuffer->getCurrentSize();
  uint8_t** nextTag = tags;
  while (buffer < end) {
    // Store this location in the buffer as a tag and advance the tag array
    *nextTag = buffer;
    ++nextTag;
    // Advance buffer to next tuple
    buffer = KeyValuePair::nextTuple(buffer);
  }

  timer.stop();
  logger.add(populateTimeStatID, timer.getElapsed());
  timer.start();

  // qsort on tag array.
  qsort(tags, numTuples, sizeof(uint8_t*), comparisonFunction);

  timer.stop();
  logger.add(qsortTimeStatID, timer.getElapsed());
  timer.start();

  // Collect sorted tuples.
  nextTag = tags;
  for (uint64_t i = 0; i < numTuples; ++i, ++nextTag) {
    outputBuffer->append(*nextTag, KeyValuePair::tupleSize(*nextTag));
  }

  timer.stop();
  logger.add(collectTimeStatID, timer.getElapsed());

  // Reset scratch buffer so that we don't ever re-use a stale one by accident
  scratchBuffer = NULL;

  sortTimer.stop();
  logger.add(sortTimeStatID, sortTimer.getElapsed());
}

uint64_t QuickSortStrategy::getRequiredScratchBufferSize(
  KVPairBuffer* buffer) const {
  return buffer->getNumTuples() * sizeof(uint8_t*);
}

void QuickSortStrategy::setScratchBuffer(uint8_t* scratchBuffer) {
  this->scratchBuffer = scratchBuffer;
}

SortAlgorithm QuickSortStrategy::getSortAlgorithmID() const {
  return QUICK_SORT;
}
