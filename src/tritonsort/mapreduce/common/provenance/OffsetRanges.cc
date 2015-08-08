#include <string.h>

#include "core/ByteOrder.h"
#include "mapreduce/common/provenance/OffsetRanges.h"

OffsetRanges::OffsetRanges()
  : rawIntegers(NULL),
    size(0),
    capacity(0) {
  // Allocate enough space for one range
  changeCapacity(RANGE_SIZE_BYTES);
}

OffsetRanges::~OffsetRanges() {
  if (rawIntegers != NULL) {
    free(rawIntegers);
    rawIntegers = NULL;
  }
}

uint64_t OffsetRanges::marshalledSize() const {
  // Base-64 encoding will turn each group of 3 bytes into 4 bytes, so number
  // of marshalled bytes is ceil(numBytes / 3) * 4, plus an additional 128 bits
  // to store the length of the encoded and decoded strings for demarshalling
  return MARSHAL_HEADER_SIZE + ((((size - 1) / 3) + 1) * 4);
}

void OffsetRanges::marshal(uint8_t* memory) const {
  uint8_t* writePtr = memory;

  // First, store the sizes of the encoded string and decoded strings so we
  // know how much to demarshal and how much memory to allocate for
  // demarshalling
  uint64_t bigEndianMarshalledSize = hostToBigEndian64(marshalledSize());
  memcpy(writePtr, &bigEndianMarshalledSize, sizeof(uint64_t));
  writePtr += sizeof(uint64_t);

  uint64_t bigEndianSize = hostToBigEndian64(size);
  memcpy(writePtr, &bigEndianSize, sizeof(uint64_t));
  writePtr += sizeof(uint64_t);

  // Base-64 encode rawIntegers into destination
  Base64::encode(reinterpret_cast<uint8_t*>(rawIntegers), size, writePtr);
}

uint64_t OffsetRanges::demarshal(uint8_t* memory) {
  uint64_t* integers = reinterpret_cast<uint64_t*>(memory);

  uint64_t marshalSize = bigEndianToHost64(*(integers));
  uint64_t decodedSize = bigEndianToHost64(*(integers + 1));

  changeCapacity(decodedSize);

  uint64_t bytesDecoded = Base64::decode(
    memory + MARSHAL_HEADER_SIZE,
    marshalSize - MARSHAL_HEADER_SIZE,
    reinterpret_cast<uint8_t*>(rawIntegers));

  ABORT_IF(bytesDecoded != decodedSize, "Expected to Base64-decode %llu bytes, "
         "but decoded %llu bytes", decodedSize, bytesDecoded);
  size = bytesDecoded;

  return marshalSize;
}

uint64_t OffsetRanges::numRanges() const {
  return size / RANGE_SIZE_BYTES;
}

void OffsetRanges::range(
  uint64_t i, uint64_t& rangeStart, uint64_t& rangeEnd) const {

  uint64_t startIndex = (RANGE_SIZE_INTS * i);

  ASSERT(startIndex + 1 < size, "Invalid range index %llu", i);

  rangeStart = bigEndianToHost64(rawIntegers[startIndex]);
  rangeEnd = bigEndianToHost64(rawIntegers[startIndex + 1]);
}

void OffsetRanges::add(uint64_t startOffset, uint64_t endOffset) {
  if (size == capacity) {
    changeCapacity(capacity * 2);
  }

  uint64_t startIndex = (size / RANGE_SIZE_BYTES) * RANGE_SIZE_INTS;
  uint64_t endIndex = startIndex + 1;

  if (size > 0) {
    uint64_t lastRangeEnd = bigEndianToHost64(rawIntegers[startIndex - 1]);

    ASSERT(lastRangeEnd <= startOffset,
           "End of last range (%llu) should be strictly before range "
           "[%llu, %llu] currently being inserted", lastRangeEnd, startOffset,
           endOffset);

    if (lastRangeEnd == startOffset) {
      // Range extends the previous range; simply extend endpoint of that range
      rawIntegers[startIndex - 1] = hostToBigEndian64(endOffset);
      return;
    }
  }

  // Range is disjoint from previous range; add a new range
  rawIntegers[startIndex] = hostToBigEndian64(startOffset);
  rawIntegers[endIndex] = hostToBigEndian64(endOffset);
  size += RANGE_SIZE_BYTES;
}

void OffsetRanges::merge(const OffsetRanges& otherRanges) {
  if (numRanges() == 0) {
    uint64_t srcNumRanges = otherRanges.numRanges();
    changeCapacity(srcNumRanges);

    memcpy(rawIntegers, otherRanges.rawIntegers, otherRanges.size);
    size = otherRanges.size;
  } else {
    OffsetRanges mergedRanges;

    uint64_t destRange = 0;
    uint64_t destNumRanges = numRanges();
    uint64_t srcRange = 0;
    uint64_t srcNumRanges = otherRanges.numRanges();

    while (destRange < destNumRanges && srcRange < srcNumRanges) {
      uint64_t srcRangeStart;
      uint64_t srcRangeEnd;
      uint64_t destRangeStart;
      uint64_t destRangeEnd;

      otherRanges.range(srcRange, srcRangeStart, srcRangeEnd);
      range(destRange, destRangeStart, destRangeEnd);

      ASSERT(destRangeEnd <= srcRangeStart || srcRangeEnd <= destRangeStart,
             "We expect that ranges for a given file will never overlap");

      if (destRangeEnd == srcRangeStart) {
        // destRange abuts srcRange, so we need to add a merged range
        mergedRanges.add(destRangeStart, srcRangeEnd);
        destRange++;
        srcRange++;
      } else if (srcRangeEnd == destRangeStart) {
        // srcRange abuts destRange, so we need to add a merged range
        mergedRanges.add(srcRangeStart, destRangeEnd);
        destRange++;
        srcRange++;
      } else if (srcRangeStart < destRangeStart) {
        // srcRange comes before destRange, so add it first
        mergedRanges.add(srcRangeStart, srcRangeEnd);
        srcRange++;
      } else {
        // destRange comes before srcRange, so add it first
        mergedRanges.add(destRangeStart, destRangeEnd);
        destRange++;
      }
    }

    // Add any left-over ranges from either list
    while (destRange < destNumRanges) {
      uint64_t destRangeStart;
      uint64_t destRangeEnd;

      range(destRange, destRangeStart, destRangeEnd);
      mergedRanges.add(destRangeStart, destRangeEnd);

      destRange++;
    }

    while (srcRange < srcNumRanges) {
      uint64_t srcRangeStart;
      uint64_t srcRangeEnd;

      otherRanges.range(srcRange, srcRangeStart, srcRangeEnd);
      mergedRanges.add(srcRangeStart, srcRangeEnd);

      srcRange++;
    }

    // Replace destRanges with the merged range set; mergedRanges' memory
    // (which used to belong to this object) will be garbage-collected when
    // mergedRanges goes out of scope
    swap(mergedRanges);
  }
}

bool OffsetRanges::equals(const OffsetRanges& otherRanges) const {
  if (size != otherRanges.size) {
    return false;
  }

  return (memcmp(rawIntegers, otherRanges.rawIntegers, size) == 0);
}

void OffsetRanges::swap(OffsetRanges& ranges) {
  uint64_t* tmpIntegers = rawIntegers;
  uint64_t tmpSize = size;
  uint64_t tmpCapacity = capacity;

  rawIntegers = ranges.rawIntegers;
  size = ranges.size;
  capacity = ranges.capacity;

  ranges.rawIntegers = tmpIntegers;
  ranges.size = tmpSize;
  ranges.capacity = tmpCapacity;
}
