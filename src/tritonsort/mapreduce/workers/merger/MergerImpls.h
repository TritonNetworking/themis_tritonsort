#ifndef MAPRED_MERGER_IMPLS_H
#define MAPRED_MERGER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/merger/Merger.h"

class MergerImpls : public ImplementationList {
public:
  MergerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Merger, "Merger");
    ADD_IMPLEMENTATION(Sink, "SinkMerger");
  }
};

#endif // MAPRED_MERGER_IMPLS_H
