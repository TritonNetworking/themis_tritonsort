#include <string.h>

#include "core/Comparison.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/PartitionBoundaries.h"

PartitionBoundaries::PartitionBoundaries(
  uint8_t* _lowerBoundKey, uint32_t _lowerBoundKeyLength,
  uint8_t* _upperBoundKey, uint32_t _upperBoundKeyLength)
  : lowerBoundKeyLength(_lowerBoundKeyLength),
    upperBoundKeyLength(_upperBoundKeyLength) {

  ABORT_IF(_lowerBoundKey == NULL, "Every partition must have a lower bound");
  lowerBoundKey = new (themis::memcheck) uint8_t[lowerBoundKeyLength];
  memcpy(lowerBoundKey, _lowerBoundKey, lowerBoundKeyLength);

  if (_upperBoundKey != NULL) {
    upperBoundKey = new (themis::memcheck) uint8_t[upperBoundKeyLength];
    memcpy(upperBoundKey, _upperBoundKey, upperBoundKeyLength);
  } else {
    upperBoundKey = NULL;
  }
}

PartitionBoundaries::~PartitionBoundaries() {
  if (lowerBoundKey != NULL) {
    delete[] lowerBoundKey;
    lowerBoundKey = NULL;
  }

  if (upperBoundKey != NULL) {
    delete[] upperBoundKey;
    upperBoundKey = NULL;
  }
}

bool PartitionBoundaries::keyIsWithinBoundaries(
  const uint8_t* key, uint32_t keyLength) const {

  if (compare(key, keyLength, lowerBoundKey, lowerBoundKeyLength) < 0) {
    return false;
  }

  if (upperBoundKey != NULL && compare(
    key, keyLength, upperBoundKey, upperBoundKeyLength) >= 0) {
    return false;
  }

  return true;
}

std::pair<const uint8_t*, uint32_t>
PartitionBoundaries::getLowerBoundKey() const {
  return std::make_pair(lowerBoundKey, lowerBoundKeyLength);
}

std::pair<const uint8_t*, uint32_t>
PartitionBoundaries::getUpperBoundKey() const {
  return std::make_pair(upperBoundKey, upperBoundKeyLength);
}
