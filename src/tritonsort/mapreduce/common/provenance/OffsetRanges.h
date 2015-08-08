#ifndef THEMIS_OFFSET_RANGES_H
#define THEMIS_OFFSET_RANGES_H

#include <stdint.h>
#include <stdlib.h>

#include "core/Base64.h"
#include "core/TritonSortAssert.h"

/**
   OffsetRanges stores a list of file offset ranges. It supports adding ranges
   and merging ranges together, as well as efficient marshalling and
   demarshalling from memory via Base64-encoding.

   OffsetRanges stores its list of ranges in a contiguous region of memory; it
   grows the array via amortized doubling similar to the method employed by
   std::vector.
 */
class OffsetRanges {
public:
  /// Constructor
  /**
     Reserves enough initial capacity for a single range
   */
  OffsetRanges();

  /// Destructor
  virtual ~OffsetRanges();

  /// Marshal this OffsetRanges into memory
  /**
     \param memory the memory region to which to marshal this object
   */
  void marshal(uint8_t* memory) const;

  /// Demarshal an OffsetRanges from memory
  /**
     \param memory the memory region from which an OffsetRanges is to be
     demarshalled. It is assumed that this memory was populated with a
     marshalled OffsetRanges by OffsetRanges::marshal

     \return the number of bytes of `memory` that were used to demarshal this
     object
   */
  uint64_t demarshal(uint8_t* memory);

  /// \return the number of ranges stored by this object
  uint64_t numRanges() const;

  /// \return the number of bytes needed to store a marshalled copy of this
  /// object
  uint64_t marshalledSize() const;

  /// Add an offset range
  /**
     If this offset range abuts the last offset range, the last offset range is
     extended rather than adding a new range.

     \warning This method assumes that any range added to it is strictly after
     any ranges that were added with previous call so OffsetRanges::add.

     \param startOffset the start of the offset range
     \param endOffset the end of the offset range
   */
  void add(uint64_t startOffset, uint64_t endOffset);

  /// Merge another OffsetRanges into this one
  /**
     Any adjacent ranges will be merged into a single range.

     \warning This method assumes that ranges in both OffsetRanges are ordered
     by startOffset, and that ranges don't overlap.

     \param otherRanges the OffsetRanges to merge into this one
   */
  void merge(const OffsetRanges& otherRanges);

  /// \return true if this OffsetRanges and otherRanges represent the same list
  /// of ranges, false otherwise
  bool equals(const OffsetRanges& otherRanges) const;
private:
  static const uint64_t MARSHAL_HEADER_SIZE = 2 * sizeof(uint64_t);
  static const uint64_t RANGE_SIZE_INTS = 2;
  static const uint64_t RANGE_SIZE_BYTES = RANGE_SIZE_INTS * sizeof(uint64_t);

  void range(uint64_t i, uint64_t& rangeStart, uint64_t& rangeEnd) const;

  inline void changeCapacity(uint64_t newCapacity) {
    if (newCapacity <= capacity) {
      return;
    }

    if (rawIntegers == NULL) {
      rawIntegers = static_cast<uint64_t*>(malloc(newCapacity));

      ABORT_IF(rawIntegers == NULL, "Failed to grow offset ranges memory to "
               "capacity %llu", newCapacity);
    } else {
      void* newRawIntegers = realloc(rawIntegers, newCapacity);

      ABORT_IF(newRawIntegers == NULL, "Failed to grow offset ranges memory to "
               "capacity %llu", newCapacity);

      rawIntegers = static_cast<uint64_t*>(newRawIntegers);
    }

    capacity = newCapacity;
  }

  void swap(OffsetRanges& ranges);

  /// The raw array used to store ranges and metadata contiguously. All ranges
  /// and metadata are stored as big-endian integers
  uint64_t* rawIntegers;
  /// The number of bytes of rawIntegers currently in use
  uint64_t size;
  /// The number of ranges that rawIntegers can hold before it's full
  uint64_t capacity;
};

#endif // THEMIS_OFFSET_RANGES_H
