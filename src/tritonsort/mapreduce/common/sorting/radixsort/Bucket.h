#ifndef TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_H
#define TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_H

#include <string.h>

#include "Constants.h"
#include "core/TritonSortAssert.h"

/**
   Bucket is a container for key entries that supports sequential reads and
   writes. A key entry is a portion of memory that contains a fixed size key and
   an offset tag locating the key in the original buffer. Keys shorter than the
   maximum key size are padded with 0s to ensure the 'fixed size' property.

   RadixSort uses 2 sets of buckets, with 256 buckets in each set.
   Each bucket holds keys with the same byte in the key at a specified offset.
 */
class Bucket {
public:
  /// Constructor
  Bucket();

  /**
     Resets the bucket for the next round of key distribution.

     \param newBufferPosition the position of the underlying buffer that this
            bucket should write to.

     \param newCapacity the number of key-entries that this bucket will hold.

     \param newMaxKeySize the maximum size of keys added to the bucket

     \param offsetSize the size of the offset tag associated with key-entries.
   */
  void reset(uint8_t* newBufferPosition, uint64_t newCapacity,
             uint32_t newMaxKeySize, OffsetSize offsetSize);

  // Keep the following implementations in the header for inlining purposes:

  /**
     Writes a key-entry to the bucket.

     \param entry the memory location of the entry to write
   */
  inline void writeEntry(uint8_t* entry) {
    ASSERT(entry != NULL, "Cannot write NULL entry to bucket");
    ASSERT(writePointer + entrySize <= endPointer,
           "Cannot write to full bucket");
    // Copy the entry to this bucket's buffer and advance the write pointer
    writePointer = reinterpret_cast<uint8_t*>(
      mempcpy(writePointer, entry, entrySize));
  }

  /**
     Write a key-entry to the bucket sourced from an input KVPairBuffer. This
     operation forms the key-entry from information about the tuple, rather than
     copying an existing entry.

     \param keyPointer the memory location of the key in the input buffer

     \param keyLength the length of the key to write

     \param offset the starting position of the tuple in the input buffer
            relative to the start of the buffer
   */
  inline void writeEntryFromBuffer(uint8_t* keyPointer, uint32_t keyLength,
                                   uint64_t offset) {
    ASSERT(writePointer + entrySize <= endPointer,
           "Cannot write to full bucket");
    ASSERT(keyLength <= maxKeySize, "Cannot write entry with key size %llu "
           "larger than max key size %llu", keyLength, maxKeySize);

    // Write the key
    writePointer = reinterpret_cast<uint8_t*>(
      mempcpy(writePointer, keyPointer, keyLength));;
    // 0-pad up to the maximum key size
    uint32_t paddingSize = maxKeySize - keyLength;
    memset(writePointer, 0, paddingSize);
    writePointer += paddingSize;

    // Write the tuple's offset
    if (offsetSize == UINT16_T) {
      // Write a shortened uint16_t offset
      uint16_t shortOffset = static_cast<uint16_t>(offset);
      writePointer = reinterpret_cast<uint8_t*>(
        mempcpy(writePointer, &shortOffset, sizeof(shortOffset)));
    } else if (offsetSize == UINT32_T) {
      // Write a shortened uint32_t offset
      uint32_t shortOffset = static_cast<uint32_t>(offset);
      writePointer = reinterpret_cast<uint8_t*>(
        mempcpy(writePointer, &shortOffset, sizeof(shortOffset)));
    } else {
      // Just write the full uin64_t offset
      writePointer = reinterpret_cast<uint8_t*>(
        mempcpy(writePointer, &offset, sizeof(offset)));
    }
  }

  /**
     Reads an entry from the bucket.

     \param entry output parameter that will be set to the address of the next
            entry in the bucket.

     \return true if an entry was successfully read and false if the bucket is
             empty
   */
  inline bool readNextEntry(uint8_t*& entry) {
    entry = readPointer;
    readPointer += entrySize;
    return readPointer <= endPointer;
  }

private:
  uint32_t maxKeySize;
  uint64_t entrySize;
  uint8_t* readPointer;
  uint8_t* writePointer;
  uint8_t* endPointer;

  OffsetSize offsetSize;
};

#endif // TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_BUCKET_H
