#ifndef _TRITONSORT_DUMMY_WORK_UNIT
#define _TRITONSORT_DUMMY_WORK_UNIT

#include "core/Resource.h"

/**
   A dummy work unit designed to do things like kickstart workers that don't
   have any initial input.
 */
class DummyWorkUnit : public Resource {
public:
  /// Constructor
  DummyWorkUnit() {}

  /// \sa Resource::getCurrentSize
  uint64_t getCurrentSize() const {
    return 0;
  }
};

#endif // _TRITONSORT_DUMMY_WORK_UNIT
