#ifndef MAPREDUCE_WORKER_IMPLS_H
#define MAPREDUCE_WORKER_IMPLS_H

#include "common/workers/coalescer/CoalescerImpls.h"
#include "common/workers/itemholder/ItemHolderImpls.h"
#include "common/workers/receiver/ReceiverImpls.h"
#include "core/ImplementationList.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/ListableKVPairBufferFactory.h"
#include "mapreduce/common/PhaseZeroOutputData.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/buffers/ListableKVPairBuffer.h"
#include "mapreduce/workers/boundarydecider/BoundaryDeciderImpls.h"
#include "mapreduce/workers/boundarydeserializer/BoundaryDeserializerImpls.h"
#include "mapreduce/workers/boundaryscanner/BoundaryScannerImpls.h"
#include "mapreduce/workers/buffercombiner/BufferCombinerImpls.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverterImpls.h"
#include "mapreduce/workers/chainer/ChainerImpls.h"
#include "mapreduce/workers/mapper/MapperImpls.h"
#include "mapreduce/workers/merger/MergerImpls.h"
#include "mapreduce/workers/reader/ReaderImpls.h"
#include "mapreduce/workers/reducer/ReducerImpls.h"
#include "mapreduce/workers/sender/SenderImpls.h"
#include "mapreduce/workers/sorter/SorterImpls.h"
#include "mapreduce/workers/tupledemux/TupleDemuxImpls.h"
#include "mapreduce/workers/writer/WriterImpls.h"

class MapReduceWorkerImpls : public ImplementationList {
public:
  MapReduceWorkerImpls() : ImplementationList() {
    ADD_STAGE_IMPLS(SenderImpls, "kv_pair_buf_sender");
    ADD_STAGE_IMPLS(ReceiverImpls<KVPairBufferFactory>, "kv_pair_buf_receiver");
    ADD_STAGE_IMPLS(
      ItemHolderImpls<PhaseZeroOutputData>, "phase_zero_output_data_holder");
    ADD_STAGE_IMPLS(
      (CoalescerImpls<ListableKVPairBuffer, KVPairBuffer,
       ListableKVPairBufferFactory>),
      "coalescer");
    ADD_STAGE_IMPLS(ChainerImpls, "chainer");
    ADD_STAGE_IMPLS(WriterImpls, "writer");
    ADD_STAGE_IMPLS(TupleDemuxImpls, "tuple_demux");
    ADD_STAGE_IMPLS(MapperImpls, "mapper");
    ADD_STAGE_IMPLS(MergerImpls, "merger");
    ADD_STAGE_IMPLS(BoundaryDeciderImpls, "boundary_decider");
    ADD_STAGE_IMPLS(BoundaryDeserializerImpls, "boundary_deserializer");
    ADD_STAGE_IMPLS(BoundaryScannerImpls, "boundary_scanner");
    ADD_STAGE_IMPLS(BufferCombinerImpls, "buffer_combiner");
    ADD_STAGE_IMPLS(ReaderImpls, "reader");
    ADD_STAGE_IMPLS(ReducerImpls, "reducer");
    ADD_STAGE_IMPLS(SorterImpls, "sorter");
    ADD_STAGE_IMPLS(ByteStreamConverterImpls, "byte_stream_converter");
  }
};

#endif // MAPREDUCE_WORKER_IMPLS_H
