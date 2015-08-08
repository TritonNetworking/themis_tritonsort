#ifndef STORAGEBENCH_WORKER_IMPLS_H
#define STORAGEBENCH_WORKER_IMPLS_H

#include "benchmarks/storagebench/workers/generator/GeneratorImpls.h"
#include "benchmarks/storagebench/workers/tagger/TaggerImpls.h"
#include "core/ImplementationList.h"

class StorageBenchWorkerImpls : public ImplementationList {
public:
  StorageBenchWorkerImpls() : ImplementationList() {
    ADD_STAGE_IMPLS(GeneratorImpls, "generator");
    ADD_STAGE_IMPLS(TaggerImpls, "tagger");
  }
};

#endif // STORAGEBENCH_WORKER_IMPLS_H
