#ifndef TRITONSORT_COMMON_CHAINER_IMPLS_H
#define TRITONSORT_COMMON_CHAINER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/chainer/Chainer.h"

/// Implementations of the Chainer stage
class ChainerImpls : public ImplementationList {
public:
  /// Declare all implementations of the Chainer stage
  ChainerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Chainer, "Chainer");
    ADD_IMPLEMENTATION(Sink, "SinkChainer");
  }
};

#endif // TRITONSORT_COMMON_CHAINER_IMPLS_H
