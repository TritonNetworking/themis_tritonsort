#ifndef MAPRED_WRITER_H
#define MAPRED_WRITER_H

#include "core/SingleUnitRunnable.h"

class BaseWriter;
class KVPairBuffer;

/**
   Writer issues blocking writes and is the default writer implementation.
 */
class Writer : public SingleUnitRunnable<KVPairBuffer> {
  WORKER_IMPL

public:
  /// Constructor
  /**
     \param phaseName the name of the phase

     \param stageName the name of the writer stage

     \param id the id of the writer worker

     \param params the global Params object for the application

     \param dependencies the injected dependencies for this stage

     \param maxWriteSize the maximum size of a write() syscall

     \param alignmentSize the alignment size for direct IO
   */
  Writer(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, NamedObjectCollection& dependencies,
    uint64_t maxWriteSize, uint64_t alignmentSize);

  /// Destructor
  virtual ~Writer();

  /**
     Write a buffer using a sequence of blocking write() calls.

     \param buffer the buffer to write
   */
  void run(KVPairBuffer* buffer);

  /**
     Tears down the BaseWriter to close files.
   */
  void teardown();

private:
  const uint64_t maxWriteSize;
  const uint64_t alignmentSize;

  StatLogger logger;
  BaseWriter& writer;

  Timer writeTimer;
  uint64_t writeTimeStatID;
};

#endif // MAPRED_WRITER_H
