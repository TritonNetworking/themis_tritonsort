#ifndef TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_RADIXSORT_H
#define TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_RADIXSORT_H

#include "BucketTable.h"
#include "Histogram.h"
#include "core/StatLogger.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

class KVPairBuffer;

/**
   RadixSort is the primary control structure for executing the radix sort
   algorithm. It requires scratch space for keys and tuple offsets. The basic
   algorithm is as follows:

   1. Read the input buffer into buckets based on the last byte in the key,
        padding keys with 0s until they reach the maximum key length. Add
        an offset tag to each key that tracks its initial position in the input
        buffer. A key together with an offset tag is called a **key-entry**
   2. Re-bucket the keys by the preceding byte in the key, preserving the order
        of keys with equal bytes.
   3. Repeat step 2 until all bytes of the key have been used to re-bucket keys.
   4. The keys are now sorted. Read the offset tags in order and copy the
        the original tuple of each key-entry to the output buffer.
 */
class RadixSort {
public:
  /// Constructor
  RadixSort(bool useSecondaryKeys);

  /**
     Executes the 4-step algorithm mentioned at the beginning of this header
     file

     \param inputBuffer the buffer containing the input data

     \param outputBuffer the buffer that will contain the sorted permutation of
     the data in inputBuffer
   */
  void sort(KVPairBuffer* inputBuffer, KVPairBuffer* outputBuffer);

  /**
     Provide radix sort with a scratch buffer for performing the sort

     \param scratchBuffer a pointer to the scratch buffer memory with which
     radix sort is provided
   */
  void setScratchBuffer(uint8_t* scratchBuffer);

  /// Determine how large of a scratch buffer will be needed to sort the given
  /// buffer
  /**
     \param buffer the buffer to be sorted

     \return the amount of scratch buffer space required to sort the buffer
   */
  uint64_t getRequiredScratchBufferSize(KVPairBuffer* buffer) const;
private:
  const bool useSecondaryKeys;

  uint8_t* scratchBuffer;

  KVPairBuffer* inputBuffer;
  KVPairBuffer* outputBuffer;
  uint32_t maxKeySize;
  OffsetSize offsetSize;
  Histogram histogram;
  int inputBuckets;
  BucketTable bucketTables[2];

  uint32_t keyOffset;

  StatLogger logger;
  uint64_t sortTimeStatID;
  uint64_t distributeFromBufferStatID;
  uint64_t distributeFromBucketStatID;
  uint64_t copyOutputTimeStatID;
  Timer timer, sortTimer;

  // Helper functions

  /**
     Initialize data structures and allocate buffers.
   */
  void setup();

  /**
     Clean up data structures and return buffers.
   */
  void teardown();

  /**
     Implements step 1 of the algorithm by distributing keys from the input
     buffer to one table of buckets using the last byte of the key. This method
     calls RadixSort::deriveInitialHistogram() to get the initial bucket
     configuration.
   */
  void distributeKeysFromInputBuffer();

  /**
     Implements step 2 (and 3) of the algorithm by distributing keys from one
     table of buckets to the other using successively more siginficant digits of
     the key.
   */
  bool distributeKeysFromBuckets();

  /**
     Implements step 4 of the algorithm by copying tuples from the input buffer
     to the output buffer using the offset tags coupled to the sorted keys.
   */
  void copyOutputToBuffer();

  /**
     Computes an initial histogram from the input buffer. This histogram is
     required to compete the first iteration of bucketing. Each bucketing
     iteration pre-computes the histogram for the next step, so this function
     only needs to be called once before bucketing begins.
   */
  void deriveInitialHistogram();

  /**
     Retrieves the BucketTable to be read from next.

     \return the input BucketTable
   */
  inline BucketTable& getInputBuckets() {
    return bucketTables[inputBuckets];
  }

  /**
     Retrieves the BucketTable to be written to next.

     \return the output BucketTable
   */
  inline BucketTable& getOutputBuckets() {
    return bucketTables[1 - inputBuckets];
  }

  /**
     Switches the roles of the input and output BucketTables, so that input
     becomes output and vice versa.
   */
  inline void swapBucketTables() {
    inputBuckets = 1 - inputBuckets;
  }

  /**
     Compute the number of bytes required to store an offset for a given buffer

     \param buffer the buffer for which to compute the number of offset bytes

     \return the number of offset bytes required to store an offset for buffer
   */
  inline OffsetSize getOffsetSize(KVPairBuffer* buffer) const {
    uint64_t inputSize = buffer->getCurrentSize();
    if (inputSize < std::numeric_limits<uint16_t>::max()) {
      // If we can pack any offset into a uint16_t, then do so to save space.
      return UINT16_T;
    } else if (inputSize < std::numeric_limits<uint32_t>::max()) {
      // If we can pack any offset into a uint32_t, then do so to save space.
      return UINT32_T;
    } else {
      // Use the full 8 bytes to store offsets
      return UINT64_T;
    }
  }

  inline uint64_t getScratchBufferEntrySize(KVPairBuffer* buffer) const {
    uint64_t keyLength = buffer->getMaxKeyLength();

    if (useSecondaryKeys) {
      keyLength += sizeof(uint64_t);
    }

    return keyLength + offsetBytes[getOffsetSize(buffer)];
  }
};

#endif // TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_RADIXSORT_H
