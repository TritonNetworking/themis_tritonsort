#ifndef TRITONSORT_NONE_H
#define TRITONSORT_NONE_H

/**
   In the templates for workers and trackers, we need to specify an input and
   an output type. This is a placeholder type for when a worker / tracker
   doesn't have an input or output (i.e. it's a source or sink in the worker
   graph).
 */

#include "TritonSortAssert.h"

class None {
public:
  /// \cond PRIVATE
  None() {
    ABORT("You should never instantiate instances of this class!");
  }
  /// \endcond
};

#endif // TRITONSORT_NONE_H
