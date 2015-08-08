#ifndef MAPRED_LIBAIO_WRITER_H
#define MAPRED_LIBAIO_WRITER_H

#include <libaio.h>
#include <map>

#include "mapreduce/workers/writer/AsynchronousWriter.h"

/**
   LibAIOWriter is an AsynchronousWriter that performs writes with the
   Linux native AIO library, libaio.
 */
class LibAIOWriter : public AsynchronousWriter {
  WORKER_IMPL

public:
  /// Constructor
  /**
     \param phaseName the name of the phase

     \param stageName the name of the writer stage

     \param id the id of the writer worker

     \param params the global Params object for the application

     \param dependencies the injected dependencies for this stage

     \param alignmentSize the alignment size for direct IO

     \param asynchronousIODepth the maximum number of in-flight write operations

     \param maxWriteSize the maximum size of an asynchronous write() call
   */
  LibAIOWriter(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, NamedObjectCollection& dependencies,
    uint64_t alignmentSize, uint64_t asynchronousIODepth,
    uint64_t maxWriteSize);

  /// Destructor
  virtual ~LibAIOWriter();

  /**
     Wait until all existing writes have completed and then tear down the
     BaseWriter to close files.
   */
  void teardown();

private:
  typedef std::map<struct iocb*, KVPairBuffer*> WriteMap;

  /// Prepare a buffer for writing with libaio
  /**
     \param buffer the buffer to write
   */
  void prepareWrite(KVPairBuffer* buffer);

  /// \sa AsynchronousWriter::issueNextWrite
  bool issueNextWrite(KVPairBuffer* buffer);

  /// \sa AsynchronousWriter::WaitForWritesToComplete
  void waitForWritesToComplete(uint64_t numWrites);

  /// \sa AsynchronousWriter::numWritesInProgress
  uint64_t numWritesInProgress();

  const uint64_t alignmentSize;
  const uint64_t maxWriteSize;

  uint64_t asynchronousIODepthStatID;

  WriteMap outstandingWriteBuffers;

  io_context_t context;
  struct io_event* events;
};

#endif // MAPRED_LIBAIO_WRITER_H
