#ifndef TRITONSORT_COMMON_COALESCER_IMPLS_H
#define TRITONSORT_COMMON_COALESCER_IMPLS_H

#include "core/ImplementationList.h"
#include "Coalescer.h"
#include "SinkCoalescer.h"

/// Information about the implementations of the Coalescer stage
template <typename In, typename Out, typename OutFactory> class CoalescerImpls
  : public ImplementationList {

public:
  /// Declare the implementations of the Coalescer stage
  CoalescerImpls() : ImplementationList() {
    typedef Coalescer<In,Out,OutFactory> Coalescer_In_Out_OutFact;
    typedef SinkCoalescer<In> SinkCoalescer_In;

    ADD_IMPLEMENTATION(Coalescer_In_Out_OutFact, "Coalescer");
    ADD_IMPLEMENTATION(SinkCoalescer_In, "SinkCoalescer");
  }
};

#endif // TRITONSORT_COMMON_COALESCER_IMPLS_H
