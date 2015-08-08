#ifndef MAPRED_BOUNDARY_DECIDER_IMPLS_H
#define MAPRED_BOUNDARY_DECIDER_IMPLS_H

#include "BoundaryDecider.h"
#include "core/ImplementationList.h"

class BoundaryDeciderImpls : public ImplementationList {
public:
  BoundaryDeciderImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(BoundaryDecider, "BoundaryDecider");
  }
};

#endif // MAPRED_BOUNDARY_DECIDER_IMPLS_H
