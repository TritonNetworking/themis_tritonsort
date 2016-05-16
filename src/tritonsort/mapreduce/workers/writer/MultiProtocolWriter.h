#ifndef THEMIS_WORKERS_WRITER_MULTI_PROTOCOL_WRITER_H
#define THEMIS_WORKERS_WRITER_MULTI_PROTOCOL_WRITER_H

/**
   MultiProtocolWriter writes data for multiple jobs using the writer
   appropriate to that job's output location.

   MultiProtocorWriter maintains a child worker for each protocol, and
   dispatches a buffer to the appropriate child worker based on the protocol
   used for its job

   NOTE: MultiProtocolWriter does not support asynchronous I/O. All writes to
   local disk use the synchronous Writer implementation.
 */

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/CoordinatorClientInterface.h"

class MultiProtocolWriter : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  MultiProtocolWriter(
    const std::string& phaseName, const std::string& stageName,
    uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
    NamedObjectCollection& dependencies);

  /// Destructor
  virtual ~MultiProtocolWriter();

  void run(KVPairBuffer* buffer);
  void teardown();

private:
  typedef SingleUnitRunnable<KVPairBuffer> WriteWorker;
  typedef std::map<uint64_t,  WriteWorker*> WriterMap;

  const std::string phaseName;
  NamedObjectCollection& dependencies;
  Params& params;
  MemoryAllocatorInterface& memoryAllocator;


  CoordinatorClientInterface* coordinatorClient;

  WriterMap writerMap;
};


#endif // THEMIS_WORKERS_WRITER_MULTI_PROTOCOL_WRITER_H
