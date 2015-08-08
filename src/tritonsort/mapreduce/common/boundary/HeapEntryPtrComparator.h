#ifndef _TRITONSORT_MAPRED_BOUNDARY_HEAPENTRYPTRCOMPARATOR_H
#define _TRITONSORT_MAPRED_BOUNDARY_HEAPENTRYPTRCOMPARATOR_H

#include <queue>
#include <vector>

#include "HeapEntry.h"

class HeapEntryPtrComparator {
public:
  bool operator() (const HeapEntry* lhs, const HeapEntry* rhs) {
    return *lhs > *rhs;
  }
};

typedef std::priority_queue<HeapEntry*, std::vector<HeapEntry*>,
                            HeapEntryPtrComparator > TupleHeap;

#endif //_TRITONSORT_MAPRED_BOUNDARY_HEAPENTRYPTRCOMPARATOR_H
