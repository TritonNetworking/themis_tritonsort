#ifndef MIXEDIOBENCH_DEMUX_IMPLS_H
#define MIXEDIOBENCH_DEMUX_IMPLS_H

#include "benchmarks/mixediobench/workers/demux/Demux.h"
#include "core/ImplementationList.h"

class DemuxImpls : public ImplementationList {
public:
  DemuxImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Demux, "Demux");
  }
};

#endif // MIXEDIOBENCH_DEMUX_IMPLS_H
