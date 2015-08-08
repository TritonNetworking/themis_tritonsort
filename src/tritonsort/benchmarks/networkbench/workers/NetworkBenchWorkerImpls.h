#ifndef NETWORKBENCH_WORKER_IMPLS_H
#define NETWORKBENCH_WORKER_IMPLS_H

#include "benchmarks/networkbench/workers/generator/GeneratorImpls.h"
#include "core/ImplementationList.h"

class NetworkBenchWorkerImpls : public ImplementationList {
public:
  NetworkBenchWorkerImpls() : ImplementationList() {
    ADD_STAGE_IMPLS(GeneratorImpls, "generator");
  }
};

#endif // NETWORKBENCH_WORKER_IMPLS_H
