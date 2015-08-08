#ifndef MAPRED_BYTE_STREAM_CONVERTER_IMPLS_H
#define MAPRED_BYTE_STREAM_CONVERTER_IMPLS_H

#include "common/workers/sink/Sink.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"

class ByteStreamConverterImpls : public ImplementationList {
public:
  ByteStreamConverterImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(ByteStreamConverter, "ByteStreamConverter");
    ADD_IMPLEMENTATION(Sink, "SinkByteStreamConverter");
  }
};

#endif // MAPRED_BYTE_STREAM_CONVERTER_IMPLS_H
