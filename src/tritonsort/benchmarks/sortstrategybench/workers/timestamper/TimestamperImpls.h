#ifndef SORT_STRATEGY_BENCH_TIMESTAMPER_IMPLS_H
#define SORT_STRATEGY_BENCH_TIMESTAMPER_IMPLS_H

#include "benchmarks/sortstrategybench/workers/timestamper/Timestamper.h"
#include "core/ImplementationList.h"

class TimestamperImpls : public ImplementationList {
public:
  TimestamperImpls()
    : ImplementationList() {
    ADD_IMPLEMENTATION(Timestamper, "Timestamper");
  }
};

#endif // SORT_STRATEGY_BENCH_TIMESTAMPER_IMPLS_H
