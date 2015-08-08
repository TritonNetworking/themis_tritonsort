#include "BucketTable.h"

BucketTable::BucketTable(Histogram& _histogram) :
  maxKeySize(0),
  offsetSize(UINT64_T),
  numEntries(0),
  entrySize(0),
  histogram(_histogram),
  buffer(NULL),
  keyOffset(0) {}

void BucketTable::resetBuckets() {
  uint64_t offset = 0;
  for (uint64_t i = 0; i < NUM_BUCKETS; ++i) {
    // Reset and resize the bucket
    uint64_t numEntries = histogram.get(i);
    buckets[i].reset(buffer + offset, numEntries, maxKeySize, offsetSize);
    // Compute the offset for the next bucket
    offset += numEntries * entrySize;
  }

  // Reset the histogram for the next iteration of Radix Sort
  histogram.reset();

  // Set the read iterator to the first bucket.
  bucketIterator = buckets;
  nonEmptyBuckets = NUM_BUCKETS;
}
