#ifndef SORT_STRATEGY_BENCH_VALIDATOR_IMPLS_H
#define SORT_STRATEGY_BENCH_VALIDATOR_IMPLS_H

#include "benchmarks/sortstrategybench/workers/validator/Validator.h"
#include "core/ImplementationList.h"

class ValidatorImpls : public ImplementationList {
public:
  ValidatorImpls()
    : ImplementationList() {
    ADD_IMPLEMENTATION(Validator, "Validator");
  }
};

#endif // SORT_STRATEGY_BENCH_VALIDATOR_IMPLS_H
