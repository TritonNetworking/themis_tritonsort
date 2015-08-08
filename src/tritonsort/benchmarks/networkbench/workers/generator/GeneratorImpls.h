#ifndef NETWORKBENCH_GENERATOR_IMPLS_H
#define NETWORKBENCH_GENERATOR_IMPLS_H

#include "benchmarks/networkbench/workers/generator/Generator.h"
#include "core/ImplementationList.h"

class GeneratorImpls : public ImplementationList {
public:
  GeneratorImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Generator, "Generator");
  }
};

#endif // NETWORKBENCH_GENERATOR_IMPLS_H
