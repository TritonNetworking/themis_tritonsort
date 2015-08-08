#ifndef MAPRED_SORTER_IMPLS_H
#define MAPRED_SORTER_IMPLS_H

#include "PhaseZeroSampleMetadataAwareSorter.h"
#include "common/workers/nop/Nop.h"
#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/sorter/Sorter.h"

class SorterImpls : public ImplementationList {
public:
  SorterImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Nop, "NopSorter");
    ADD_IMPLEMENTATION(
      PhaseZeroSampleMetadataAwareSorter, "PhaseZeroSampleMetadataAwareSorter");
    ADD_IMPLEMENTATION(Sink, "SinkSorter");
    ADD_IMPLEMENTATION(Sorter, "Sorter");
  }
};

#endif // MAPRED_SORTER_IMPLS_H
