#ifndef TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_TABLE_H
#define TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_TABLE_H

#include "Bucket.h"
#include "Histogram.h"

/**
   BucketTable is a table 256 buckets, one for each byte, that supports
   reading and writing to those buckets. Write operations use an offset into the
   key to pick the bucket that gets written. The read operation reads one bucket
   completely before moving on to the next. The BucketTable maintains a
   reference to a shared histogram that is used to set bucket capacities prior
   to writing new entries.

   RadixSort uses 2 BucketTables, one for input and one for output. The
   roles of the BucketTables are reversed at each iteration of the sort, so
   that the input BucketTable in round R becomes the output
   BucketTable in round R+1.
 */
class BucketTable {
public:
  /// Constructor
  /**
     \param histogram a reference to the shared histogram object used by all
            instances of this class
   */
  BucketTable(Histogram& histogram);

  /**
     Sets the number of entries in total across all buckets.

     \param numEntries the total number of entries
   */
  inline void setNumEntries(uint64_t numEntries) {
    this->numEntries = numEntries;
  }

  /**
     Sets the maximum key size of any key to be sorted

     \param maxKeySize the maximum size a key
   */
  inline void setMaxKeySize(uint32_t maxKeySize) {
    this->maxKeySize = maxKeySize;
    // Update the entry size to reflect new maximum key size.
    entrySize = maxKeySize + offsetBytes[offsetSize];
  }

  /**
     Sets the size of the offset tag for a key-entry. Offsets are defined in
     Constants.h to be UINT16_T, UINT32_T, or UINT64_T

     \param offsetSize the size of the offset tag
   */
  inline void setOffsetSize(OffsetSize offsetSize) {
    this->offsetSize = offsetSize;
    // Update the entry size to reflect new offset size.
    entrySize = maxKeySize + offsetBytes[offsetSize];
  }

  /**
     Sets the offset into the key, ie, the current radix. This is the position
     of the byte used for bucketing in this iteration of the sort.

     \param keyOffset the offset into the key to be used for bucketing
   */
  inline void setKeyOffset(uint32_t keyOffset) {
    this->keyOffset = keyOffset;
  }

  /**
     Sets the scratch buffer used to store keys and tuple offsets.

     \param buffer a byte buffer used for scratch space
   */
  inline void setBuffer(uint8_t* buffer) {
    this->buffer = buffer;
  }

  /**
     Resets each bucket managed by the BucketTable using the shared
     histogram.
   */
  void resetBuckets();

  // Keep the following implementations in the header for inlining purposes:

  /**
     Adds an entry to the BucketTable and updates the histogram for the
     next iteration of bucketing.

     \param entry the key-entry to add
   */
  inline void addEntry(uint8_t* entry) {
    // Add to the appropriate bucket based on the current keyOffset
    buckets[getCurrentByte(entry)].writeEntry(entry);
    // Update the histogram for the next iteration
    histogram.increment(getNextByte(entry));
  }

  /**
     Adds an entry to the BucketTable but does NOT update the histogram.
     Call this method on the last iteration of bucketing.

     \param entry the key-entry to add
   */
  inline void addEntryForLastIteration(uint8_t* entry) {
    // Add to the appropriate bucket
    buckets[getCurrentByte(entry)].writeEntry(entry);
    // Don't update the historgram since this is the last iteration
  }

  /**
     Adds an entry to the BucketTable sourced from the input buffer. Since
     the 0-padded key-entry has not yet been created, this method handles short
     keys correctly by binning them in the 0-byte bucket. This method also
     updates the histogram for the next iteration. Call this method during the
     the first iteration of bucketing, where keys are sourced from a
     KVPairBuffer.

     \param keyPointer the memory location of the key in the input buffer

     \param keyLength the length of the key to write

     \param offset the starting position of the tuple in the input buffer
            relative to the start of the buffer
   */
  inline void addEntryFromBuffer(uint8_t* keyPointer, uint32_t keyLength,
                                 uint64_t offset) {
    // 0-pad bytes beyond key
    uint8_t byte = keyOffset < keyLength ? getCurrentByte(keyPointer) : 0;
    // Add to the appropriate bucket
    buckets[byte].writeEntryFromBuffer(keyPointer, keyLength, offset);
    // 0-pad bytes beyond key
    uint8_t nextByte = keyOffset - 1 < keyLength ?
      getNextByte(keyPointer) : 0;
    // Update the histogram for the next iteration
    histogram.increment(nextByte);
  }

  /**
     Reads the next entry from the BucketTable. This method attempts to
     read entries from the first bucket until it runs out and then moves onto
     the next, and so on.

     \param entry output parameter that will be set to the address of the next
            entry in the BucketTable

     \return true if an entry was successfully read and false if all buckets are
             empty
   */
  inline bool readNextEntry(uint8_t*& entry) {
    while (nonEmptyBuckets > 0) {
      if (bucketIterator->readNextEntry(entry)) {
        // Successfully read a bucket entry
        return true;
      }
      // This bucket is empty. Advance the iterator, decrement the counter, and
      // go for another iteration of the while loop.
      ++bucketIterator;
      --nonEmptyBuckets;
    }

    // Could not find any buckets with entries
    return false;
  }

private:
  // Helper functions

  /**
     Returns the byte in the key at the current keyOffset, or radix

     \param key the key to be bucketed

     \return the byte in the key at the current keyOffset
   */
  inline uint8_t getCurrentByte(uint8_t* key) {
    return key[keyOffset];
  }

  /**
     Returns the byte in the key for the next iteration of the radix sort, which
     is the byte just before keyOffset since radix sort starts from the least
     significant digit and moves backwards through the key.

     \param key the key to be bucketed

     \return the byte in the key immediately preceding keyOffset
   */
  inline uint8_t getNextByte(uint8_t* key) {
    return key[keyOffset - 1];
  }

  Bucket buckets[NUM_BUCKETS];

  uint32_t maxKeySize;
  OffsetSize offsetSize;
  uint64_t numEntries;
  uint64_t entrySize;
  Histogram& histogram;
  uint8_t* buffer;

  uint32_t keyOffset;

  // Iterator for reading entries from buckets
  Bucket* bucketIterator;
  uint64_t nonEmptyBuckets;
};

#endif // TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_TABLE_H
