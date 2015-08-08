#ifndef MAPRED_MAPPER_IMPLS_H
#define MAPRED_MAPPER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/mapper/KeysOnlySingleBufferMapper.h"
#include "mapreduce/workers/mapper/Mapper.h"
#include "mapreduce/workers/mapper/MultiJobMapper.h"
#include "mapreduce/workers/mapper/SyntheticMapper.h"

class MapperImpls : public ImplementationList {
public:
  MapperImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Mapper, "Mapper");
    ADD_IMPLEMENTATION(KeysOnlySingleBufferMapper,
                       "KeysOnlySingleBufferMapper");
    ADD_IMPLEMENTATION(SyntheticMapper, "SyntheticMapper");
    ADD_IMPLEMENTATION(Sink, "SinkMapper");
    ADD_IMPLEMENTATION(MultiJobMapper, "MultiJobMapper");
  }
};

#endif // MAPRED_MAPPER_IMPLS_H
