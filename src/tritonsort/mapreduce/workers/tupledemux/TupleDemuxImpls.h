#ifndef MAPRED_TUPLE_DEMUX_IMPLS_H
#define MAPRED_TUPLE_DEMUX_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/multijobdemux/MultiJobDemux.h"
#include "mapreduce/workers/tupledemux/TupleDemux.h"

class TupleDemuxImpls : public ImplementationList {
public:
  TupleDemuxImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(MultiJobDemux<TupleDemux>, "MultiJobTupleDemux");
    ADD_IMPLEMENTATION(Sink, "SinkTupleDemux");
    ADD_IMPLEMENTATION(TupleDemux, "TupleDemux");
  }
};

#endif // MAPRED_TUPLE_DEMUX_IMPLS_H
