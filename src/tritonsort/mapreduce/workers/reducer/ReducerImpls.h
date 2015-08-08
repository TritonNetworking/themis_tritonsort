#ifndef MAPRED_REDUCER_IMPLS_H
#define MAPRED_REDUCER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/multijobdemux/MultiJobDemux.h"
#include "mapreduce/workers/reducer/Reducer.h"

class ReducerImpls : public ImplementationList {
public:
  ReducerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(MultiJobDemux<Reducer>, "MultiJobReducer");
    ADD_IMPLEMENTATION(Reducer, "Reducer");
    ADD_IMPLEMENTATION(Sink, "SinkReducer");
  }
};

#endif // MAPRED_REDUCER_IMPLS_H
