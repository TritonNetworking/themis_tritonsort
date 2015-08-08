#ifndef MAPRED_ASYNCHRONOUS_WRITER_H
#define MAPRED_ASYNCHRONOUS_WRITER_H

#include "core/MultiQueueRunnable.h"

class BaseWriter;
class KVPairBuffer;

/**
   AsynchronousWriter is a base class for PosixAIOWriter and LibAIOWriter. It
   implements the structural logic for an AIO writer as a MultiQueueRunnable. It
   issues writes asynchronously rather than blocking until writes complete. The
   parameter ASYNCHRONOUS_IO_DEPTH.phase_name.writer determines how many writes
   will be in flight at any given time. After this many writes have been issued,
   the writer waits until at least one write completes before issuing more.

   If there are no more work units in the queue, AsynchronousWriter continues to
   process its existing buffers while checking in with the tracker at 1ms
   intervals. If the writer has no buffers to write and there are no buffers in
   the queue, it sleeps, while again checking in at 1ms intervals.
 */
class AsynchronousWriter : public MultiQueueRunnable<KVPairBuffer> {

public:
  /// Constructor
  /**
     \param phaseName the name of the phase

     \param stageName the name of the writer stage

     \param id the id of the writer worker

     \param params the global Params object for the application

     \param dependencies the injected dependencies for this stage

     \param asyncMode open mode for file - should be either WRITE_LIBAIO
     or WRITE_POSIXAIO

     \param asynchronousIODepth the maximum number of in-flight write operations
   */
  AsynchronousWriter(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, NamedObjectCollection& dependencies,
    File::AccessMode asyncMode, uint64_t asynchronousIODepth);

  /// Destructor
  virtual ~AsynchronousWriter();

  /// Issue writes asynchronously up to the given IO depth.
  void run();

  /// Checks that all writes have been completed successfully, and teardown the
  /// BaseWriter
  virtual void teardown();

protected:
  /**
     Issue the next write for a given buffer.

     \param buffer the buffer to write

     \return true if a write was issued, and false if the buffer has been
     completely written to the file
   */
  virtual bool issueNextWrite(KVPairBuffer* buffer) = 0;

  /// Wait for some writes to complete.
  /**
     \param numWrites the number of writes that must complete before this
     returns.
   */
  virtual void waitForWritesToComplete(uint64_t numWrites) = 0;

  /// Issue new writes for any buffers that are idle but still not complete.
  void serviceIdleBuffers();

  const uint64_t asynchronousIODepth;

  StatLogger logger;

  std::queue<KVPairBuffer*> idleBuffers;

  BaseWriter& writer;

private:
  /// Prepare a buffer for asynchronous writing.
  /**
     \param buffer the buffer to write
   */
  virtual void prepareWrite(KVPairBuffer* buffer) = 0;

  /// \return the number of writes that are in-progress
  virtual uint64_t numWritesInProgress() = 0;

  /// Wait for at least one write to complete.
  void waitForWriteToComplete();

  uint64_t asynchronousIODepthStatID;
};

#endif // MAPRED_ASYNCHRONOUS_WRITER_H
