#ifndef SORT_STRATEGY_BENCH_WORKER_IMPLS_H
#define SORT_STRATEGY_BENCH_WORKER_IMPLS_H

#include "benchmarks/sortstrategybench/workers/timestamper/TimestamperImpls.h"
#include "benchmarks/sortstrategybench/workers/validator/ValidatorImpls.h"
#include "core/ImplementationList.h"

class SortStrategyBenchWorkerImpls : public ImplementationList {
public:
  SortStrategyBenchWorkerImpls()
    : ImplementationList() {
    ADD_STAGE_IMPLS(TimestamperImpls, "timestamper");
    ADD_STAGE_IMPLS(ValidatorImpls, "validator");
  }
};

#endif // SORT_STRATEGY_BENCH_WORKER_IMPLS_H
