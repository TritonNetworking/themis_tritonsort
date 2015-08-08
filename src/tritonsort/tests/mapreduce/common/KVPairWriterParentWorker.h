#ifndef MAPRED_KVPAIR_WRITER_PARENT_WORKER_H
#define MAPRED_KVPAIR_WRITER_PARENT_WORKER_H

#include <queue>

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/KVPairBufferFactory.h"

/**
   A stub class designed to provide the necessary upcalls to test KVPairWriter
 */
class KVPairWriterParentWorker : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  KVPairWriterParentWorker(
    MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize);

  /// Aborts immediately, since this stage isn't meant to be run
  void run(KVPairBuffer* workUnit);

  /// Retrieves a buffer from the worker's pool
  KVPairBuffer* getBufferForWriter(uint64_t minCapacity);

  /// Returns a buffer to the worker's pool
  void putBufferFromWriter(KVPairBuffer* buffer);

  /// Stores the buffer in an internal list so it can be retrieved and looked
  /// at by tests later.
  void emitBufferFromWriter(KVPairBuffer* buffer, uint64_t bufferNumber);

  /// Get the list of buffers that the KVPairWriter told the worker to emit.
  /**
     \return a reference to the emitted buffer list
   */
  const std::list<KVPairBuffer*>& getEmittedBuffers() const;

  /// Put all the buffers in the emitted buffer list back in the buffer pool
  void returnEmittedBuffersToPool();

  void logSample(KeyValuePair& sample) const {
    // No-op
  }

  void logWriteStats(
    uint64_t bytesWritten, uint64_t bytesSeen, uint64_t tuplesWritten,
    uint64_t tuplesSeen) {
    // No-op
  }
private:
  const uint64_t defaultBufferSize;
  KVPairBufferFactory bufferFactory;
  std::list<KVPairBuffer*> emittedBuffers;
};

#endif // MAPRED_KVPAIR_WRITER_PARENT_WORKER_H
