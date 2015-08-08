#ifndef TRITONSORT_MAPREDUCE_QUICK_SORT_STRATEGY_H
#define TRITONSORT_MAPREDUCE_QUICK_SORT_STRATEGY_H

#include "core/Comparison.h"
#include "core/StatLogger.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/sorting/SortStrategyInterface.h"

/**
   A sort strategy implementation that uses C's qsort() routine to sort tuples
   by key. A buffer of tags is constructed from scratch space provided by an
   external source (typically a Sorter worker). Tags are (8-byte) pointers to
   tuples in the original input buffer.

   QuickSortStrategy can achieve 95 MBps on graysort input data.
 */
class QuickSortStrategy : public SortStrategyInterface {
public:
  /// Constructor
  QuickSortStrategy(bool useSecondaryKeys);

  /**
     Executes the quicksort using qsort().

     \sa SortStrategyInterface::sort
   */
  void sort(KVPairBuffer* inputBuffer, KVPairBuffer* outputBuffer);

  /**
     QuickSort requires a uint8_t* for each tuple in the buffer.

     \sa SortStrategyInterface::getRequiredScratchBufferSize
   */
  uint64_t getRequiredScratchBufferSize(KVPairBuffer* buffer) const;

  /// \sa SortStrategyInterface::setScratchBuffer
  void setScratchBuffer(uint8_t* scratchBuffer);

  /// \sa SortStrategyInterface::getSortAlgorithmID
  SortAlgorithm getSortAlgorithmID() const;
private:
  typedef int (*KeyComparisonFunction)(const void*, const void*);

  /**
     Comparator function for qsort() that sorts uint8_t* tags.

     \param tag1 a pointer to a tuple in the input buffer

     \param tag2 a pointer to a tuple in the input buffer

     \return a negative number if the tuple referenced by tag1 is smaller, a
     positive number if the tuple referenced by tag2 is smaller, and 0 if both
     tuples are equal
   */
  inline static int compareTags(const void* tag1, const void* tag2) {
    // Interpret tags as tuple pointers
    uint8_t* tuple1 = *static_cast<uint8_t**>(const_cast<void*>(tag1));
    uint8_t* tuple2 = *static_cast<uint8_t**>(const_cast<void*>(tag2));

    // Compare keys
    return compare(
      KeyValuePair::key(tuple1), KeyValuePair::keyLength(tuple1),
      KeyValuePair::key(tuple2), KeyValuePair::keyLength(tuple2));
  }

  /**
     Comparator function for qsort() that sorts uint8_t* tags.

     \param tag1 a pointer to a tuple in the input buffer

     \param tag2 a pointer to a tuple in the input buffer

     \return a negative number if the tuple referenced by tag1 is smaller, a
     positive number if the tuple referenced by tag2 is smaller, and 0 if both
     tuples are equal
   */
  inline static int compareTagsWithSecondaryKey(
    const void* tag1, const void* tag2) {

    // Interpret tags as tuple pointers
    uint8_t* tuple1 = *static_cast<uint8_t**>(const_cast<void*>(tag1));
    uint8_t* tuple2 = *static_cast<uint8_t**>(const_cast<void*>(tag2));

    // Compare keys
    return compare(
      KeyValuePair::key(tuple1),
      KeyValuePair::keyLength(tuple1) + sizeof(uint64_t),
      KeyValuePair::key(tuple2),
      KeyValuePair::keyLength(tuple2) + sizeof(uint64_t));
  }

  KeyComparisonFunction comparisonFunction;

  uint8_t* scratchBuffer;
  // Logging
  StatLogger logger;
  uint64_t sortTimeStatID;
  uint64_t populateTimeStatID;
  uint64_t qsortTimeStatID;
  uint64_t collectTimeStatID;
};

#endif //TRITONSORT_MAPREDUCE_QUICK_SORT_STRATEGY_H
