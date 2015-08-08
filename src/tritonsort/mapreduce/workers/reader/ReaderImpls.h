#ifndef MAPRED_READER_IMPLS_H
#define MAPRED_READER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "core/ImplementationList.h"
#include "mapreduce/workers/reader/ByteStreamReader.h"
#include "mapreduce/workers/reader/LibAIOReader.h"
#include "mapreduce/workers/reader/MultiProtocolReader.h"
#include "mapreduce/workers/reader/PosixAIOReader.h"
#include "mapreduce/workers/reader/WholeFileReader.h"

class ReaderImpls : public ImplementationList {
public:
  ReaderImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(ByteStreamReader, "ByteStreamReader");
    ADD_IMPLEMENTATION(LibAIOReader, "LibAIOReader");
    ADD_IMPLEMENTATION(MultiProtocolReader, "MultiProtocolReader");
    ADD_IMPLEMENTATION(PosixAIOReader, "PosixAIOReader");
    ADD_IMPLEMENTATION(Sink, "SinkReader");
    ADD_IMPLEMENTATION(WholeFileReader, "WholeFileReader");
  }
};

#endif // MAPRED_READER_IMPLS_H
