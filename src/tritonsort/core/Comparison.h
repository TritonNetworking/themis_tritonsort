#ifndef THEMIS_COMPARISON_H
#define THEMIS_COMPARISON_H

#include <algorithm>
#include <stdint.h>

// General purpose comparator for byte sequences.  If sequences are identical
// up to the minimum length, the shorter sequence is considered smaller.
// Returns:
// < 0 if data1 < data2
// = 0 if data1 == data2
// > 0 if data1 > data2
//
// Note: memcmp() was removed for speed
static inline int compare(
  const uint8_t* data1, uint64_t len1, const uint8_t* data2, uint32_t len2) {
  // Iterate byte by byte up to the length of the smaller datum, checking
  // ordering
  for (uint32_t minLength = std::min<uint32_t>(len1, len2); minLength != 0;
       ++data1, ++data2, --minLength) {
    if (*data1 != *data2) {
      return *data1 - *data2;
    }
  }

  // Both byte streams are equal up to minLength
  // Order the shorter one before the longer
  return len1 - len2;
}

#endif // THEMIS_COMPARISON_H
