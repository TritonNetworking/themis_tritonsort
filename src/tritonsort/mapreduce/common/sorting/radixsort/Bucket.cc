#include "Bucket.h"

Bucket::Bucket() :
  maxKeySize(0),
  entrySize(0),
  readPointer(NULL),
  writePointer(NULL),
  endPointer(NULL),
  offsetSize(UINT64_T) {}

void Bucket::reset(uint8_t* newBufferPosition, uint64_t newCapacity,
                   uint32_t newMaxKeySize, OffsetSize offsetSize) {
  readPointer = newBufferPosition;
  writePointer = newBufferPosition;
  maxKeySize = newMaxKeySize;
  // An entry is a key and an offset tag
  entrySize = maxKeySize + offsetBytes[offsetSize];
  endPointer = newBufferPosition + (entrySize * newCapacity);
  this->offsetSize = offsetSize;
}
