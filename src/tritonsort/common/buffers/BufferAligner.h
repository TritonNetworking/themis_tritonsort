#ifndef TRITONSORT_BUFFER_ALIGNER_H
#define TRITONSORT_BUFFER_ALIGNER_H

#include <map>
#include <new>
#include <string.h>

#include "core/TritonSortAssert.h"

/**
   A buffer aligner is an auxiliary data structure that enables a stage to emit
   only perfectly O_DIRECT aligned buffers. It has a number of slots that are
   accessed by buffer numbers. A stage should utilize the aligner roughly as
   follows:

   1. Check out a buffer.
   2. Call BufferAligner::prepare() to copy old bytes to the front of the buffer
   3. Append its own data to the buffer.
   4. Call BufferAligner::finish() to copy unaligned bytes to the aligner and
      truncate the buffer so it is aligned.
   5. Emit the buffer.

   The very last buffer passed through the aligner for a slot should use
   BufferAligner::finishLastBuffer() rather than BufferAligner::finish(). This
   will leave the final buffer unaligned and leave the BufferAligner's internal
   data structures in a consistent state. Note that because finishLastBuffer()
   does not modify the buffer, it only requires the slot number as an argument.

   During teardown, a stage can check if there are any unaligned bytes left in
   the aligner by calling BufferAligner::hasRemainingBytes(). The stage should
   use prepare/finishLastBuffer as described above to emit these final bytes
   unaligned.
*/
template <typename T> class BufferAligner {
public:
  /// Constructor
  /**
     \param alignmentMultiple_ the smallest size acceptable for direct IO
   */
  BufferAligner(uint64_t alignmentMultiple_) :
    alignmentMultiple(alignmentMultiple_) {}

  /// Destructor
  virtual ~BufferAligner() {
    // Free misalignedBytesTable memory
    for (typename MisalignedBytesTable::iterator iter =
           misalignedBytesTable.begin();
         iter != misalignedBytesTable.end(); ++iter) {
      // There should not be any misaligned bytes left.
      ABORT_IF(iter->second.numMisalignedBytes != 0,
               "There were %llu misaligned bytes left for buffer %llu."
               "Should be 0", iter->second.numMisalignedBytes, iter->first);
      // Delete the actual byte buffer
      delete[] iter->second.misalignedBytes;
    }
  }

  /**
     Prepares a buffer for normal append operation by the parent stage. Under
     the hood, this function copies left over bytes from the last buffer to the
     front of this buffer.

     \param buffer the buffer to prepare

     \param bufferNumber the slot for this buffer. Could be logical disk ID or
     something else.
   */
  void prepare(T* buffer, uint64_t bufferNumber) {
    TRITONSORT_ASSERT(buffer != NULL, "prepare() got NULL buffer");

    // Make sure this buffer number is tracked.

    typename MisalignedBytesTable::iterator tableIter =
      misalignedBytesTable.find(bufferNumber);

    TableEntry* entry = NULL;

    if (tableIter == misalignedBytesTable.end()) {
      misalignedBytesTable[bufferNumber].misalignedBytes = newByteArray();
      entry = &(misalignedBytesTable[bufferNumber]);
    } else {
      entry = &(tableIter->second);
    }

    // Copy table entry to the beginning of the buffer.
    buffer->append(entry->misalignedBytes, entry->numMisalignedBytes);
  }

  /**
     'Finishes' the buffer so it can be emitted to the next stage. Under the
     hood, this function copies the unaligned bytes to internal storage and then
     truncates the buffer at its largest aligned size.

     \param buffer the buffer to finish

     \param bufferNumber the slot for this buffer. Could be logical disk ID or
     something else.
   */
  void finish(T* buffer, uint64_t bufferNumber) {
    TRITONSORT_ASSERT(buffer != NULL, "finish() got NULL buffer");
    TRITONSORT_ASSERT(misalignedBytesTable.count(bufferNumber),
           "finish() got unknown buffer number %llu. Call prepare() first.",
           bufferNumber);
    // Compute misaligned region
    uint64_t misalignedBytesInBuffer =
      buffer->getCurrentSize() % alignmentMultiple;
    uint64_t alignedOffset = buffer->getCurrentSize() - misalignedBytesInBuffer;

    // Copy misaligned bytes to the table
    misalignedBytesTable[bufferNumber].numMisalignedBytes =
      misalignedBytesInBuffer;
    memcpy(misalignedBytesTable[bufferNumber].misalignedBytes,
           buffer->getRawBuffer() + alignedOffset, misalignedBytesInBuffer);

    // Truncate the buffer at its aligned offset
    buffer->setCurrentSize(alignedOffset);
  }

  /**
     Finish the last buffer for a given slot. This function does not truncate
     the buffer, but instead just records that the slot is done in internal
     storage, by setting the number of unaligned bytes to 0.

     /param the buffer number that has been completed by the parent stage
   */
  void finishLastBuffer(uint64_t bufferNumber) {
    typename MisalignedBytesTable::iterator tableIter =
      misalignedBytesTable.find(bufferNumber);

    if (tableIter != misalignedBytesTable.end()) {
      // Zero the number of misaligned bytes
      (tableIter->second).numMisalignedBytes = 0;
    }
  }

  /**
     \param bufferNumber the slot to check for remaining bytes

     \return the number of misaligned bytes left for
     a given buffer number
   */
  uint64_t getRemainingBytes(uint64_t bufferNumber) {
    typename MisalignedBytesTable::iterator tableIter =
      misalignedBytesTable.find(bufferNumber);

    if (tableIter != misalignedBytesTable.end()) {
      return (tableIter->second).numMisalignedBytes;
    } else {
      return 0;
    }
  }

  /**
     Utility function for determining if there are misaligned bytes left for
     a given buffer number. This function can be used to tell if a stage needs
     to call prepare/markLastBuffer for a given buffer number.

     \param bufferNumber the slot to check for remaining bytes

     \return true if there are bytes stored internally and false otherwise
   */
  bool hasRemainingBytes(uint64_t bufferNumber) {
    typename MisalignedBytesTable::iterator tableIter =
      misalignedBytesTable.find(bufferNumber);

    if (tableIter != misalignedBytesTable.end()) {
      return (tableIter->second).numMisalignedBytes > 0;
    } else {
      return false;
    }
  }

private:
  /**
     Utility function for creating misaligned byte arrays for TableEntrys

     \return a byte array of size alignmentMultiple - 1
   */
  uint8_t* newByteArray() {
    uint8_t* buffer = new (std::nothrow) uint8_t[alignmentMultiple - 1];
    ABORT_IF(buffer == NULL, "Could not allocate a new byte array");
    return buffer;
  }

  // Data is stored internally in a MisalignedBytesTable, which is a map from
  // buffer numbers to TableEntry structs.
  struct TableEntry {
    TableEntry() :
      numMisalignedBytes(0),
      misalignedBytes(NULL) {}

    uint64_t numMisalignedBytes;
    uint8_t* misalignedBytes;
  };
  typedef std::map<uint64_t, TableEntry> MisalignedBytesTable;

  uint64_t alignmentMultiple;
  MisalignedBytesTable misalignedBytesTable;
};

#endif // TRITONSORT_BUFFER_ALIGNER_H
