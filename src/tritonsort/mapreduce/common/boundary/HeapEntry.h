#ifndef _TRITONSORT_MAPRED_BOUNDARY_HEAPENTRY_H
#define _TRITONSORT_MAPRED_BOUNDARY_HEAPENTRY_H

#include "mapreduce/common/KeyValuePair.h"

class HeapEntry {
public:
  HeapEntry(const KeyValuePair& _tuple) : tuple(_tuple){}

  uint64_t id;
  const KeyValuePair& tuple;

  inline bool operator>(const HeapEntry& comp) const {
    return KeyValuePair::compareByKey(tuple, comp.tuple) > 0;
  }
};

#endif //_TRITONSORT_MAPRED_BOUNDARY_HEAPENTRY_H
