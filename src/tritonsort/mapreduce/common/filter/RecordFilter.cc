#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/PartitionBoundaries.h"
#include "mapreduce/common/filter/RecordFilter.h"

RecordFilter::RecordFilter() {
}

RecordFilter::~RecordFilter() {
  for (PartitionBoundaryVector::iterator iter = boundaries.begin();
       iter != boundaries.end(); iter++) {
    delete *iter;
  }

  boundaries.clear();
}

void RecordFilter::addPartitionBoundaries(
  PartitionBoundaries& boundariesToAdd) {

  boundaries.push_back(&boundariesToAdd);
}

bool RecordFilter::pass(const uint8_t* key, uint32_t keyLength) const {
  ASSERT(boundaries.size() > 0, "Shouldn't be using a record filter with no "
         "partition boundaries");

  for (PartitionBoundaryVector::const_iterator iter = boundaries.begin();
       iter != boundaries.end(); iter++) {
    if ((*iter)->keyIsWithinBoundaries(key, keyLength)) {
      return true;
    }
  }

  return false;
}

