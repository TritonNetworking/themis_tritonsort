#ifndef STORAGEBENCH_GENERATOR_IMPLS_H
#define STORAGEBENCH_GENERATOR_IMPLS_H

#include "benchmarks/storagebench/workers/generator/Generator.h"
#include "core/ImplementationList.h"

class GeneratorImpls : public ImplementationList {
public:
  GeneratorImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Generator, "Generator");
  }
};

#endif // STORAGEBENCH_GENERATOR_IMPLS_H
