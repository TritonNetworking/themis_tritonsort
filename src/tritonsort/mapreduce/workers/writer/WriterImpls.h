#ifndef THEMIS_MAPRED_WRITER_IMPLS_H
#define THEMIS_MAPRED_WRITER_IMPLS_H

#include "core/ImplementationList.h"
#include "mapreduce/common/hdfs/HDFSWriter.h"
#include "mapreduce/workers/writer/LibAIOWriter.h"
#include "mapreduce/workers/writer/MultiProtocolWriter.h"
#include "mapreduce/workers/writer/PosixAIOWriter.h"
#include "mapreduce/workers/writer/SinkWriter.h"
#include "mapreduce/workers/writer/Writer.h"

class WriterImpls : public ImplementationList {
public:
  WriterImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(HDFSWriter, "HDFSWriter");
    ADD_IMPLEMENTATION(LibAIOWriter, "LibAIOWriter");
    ADD_IMPLEMENTATION(MultiProtocolWriter, "MultiProtocolWriter");
    ADD_IMPLEMENTATION(PosixAIOWriter, "PosixAIOWriter");
    ADD_IMPLEMENTATION(SinkWriter<KVPairBuffer>, "SinkWriter");
    ADD_IMPLEMENTATION(Writer, "Writer");
  }
};

#endif // THEMIS_MAPRED_WRITER_IMPLS_H
