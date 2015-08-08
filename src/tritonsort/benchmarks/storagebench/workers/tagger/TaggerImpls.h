#ifndef STORAGEBENCH_TAGGER_IMPLS_H
#define STORAGEBENCH_TAGGER_IMPLS_H

#include "benchmarks/storagebench/workers/tagger/Tagger.h"
#include "core/ImplementationList.h"

class TaggerImpls : public ImplementationList {
public:
  TaggerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Tagger, "Tagger");
  }
};

#endif // STORAGEBENCH_TAGGER_IMPLS_H
