#ifndef MIXEDIOBENCH_WORKER_IMPLS_H
#define MIXEDIOBENCH_WORKER_IMPLS_H

#include "benchmarks/mixediobench/workers/demux/DemuxImpls.h"
#include "core/ImplementationList.h"

class MixedIOBenchWorkerImpls : public ImplementationList {
public:
  MixedIOBenchWorkerImpls() : ImplementationList() {
    ADD_STAGE_IMPLS(DemuxImpls, "demux");
  }
};

#endif // MIXEDIOBENCH_WORKER_IMPLS_H
