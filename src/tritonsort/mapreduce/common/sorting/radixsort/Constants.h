#ifndef TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_CONSTANTS_H
#define TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_CONSTANTS_H

#include <stdint.h>

// Bucketing by byte requires 256 buckets
const uint32_t NUM_BUCKETS = 256;

// Offset Sizes
//
// RadixSort pairs offset tags with key entries to denote the position of the
// key's tuple in the original buffer.  If the buffer is sufficiently small,
// RadixSort can save memory by using a uint16_t or uint32_t offset, using
// 2 bytes or 4 bytes respectively. If the buffer is larger than 4GB, the offset
// tag will be the full 8 bytes.
enum OffsetSize {UINT16_T, UINT32_T, UINT64_T};
const uint64_t offsetBytes[3] = {2, 4, 8};

#endif // TRITONSORT_MAPREDUCE_SORTING_RADIXSORT_CONSTANTS_H
