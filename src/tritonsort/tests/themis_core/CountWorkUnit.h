#ifndef TRITONSORT_TESTS_COUNT_WORK_UNIT_H
#define TRITONSORT_TESTS_COUNT_WORK_UNIT_H

#include <stdint.h>

#include "core/Resource.h"

class CountWorkUnit : public Resource {
public:
  CountWorkUnit(uint64_t _count) : count(_count) {
  }

  uint64_t getCurrentSize() const {
    return sizeof(uint64_t);
  }

  const uint64_t count;
};

#endif // TRITONSORT_TESTS_COUNT_WORK_UNIT_H
