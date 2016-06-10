#ifndef MIXEDIOBENCH_DEMUX_H
#define MIXEDIOBENCH_DEMUX_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   Demux is a worker that substitutes for the normal TupleDemux worker that gets
   placed after a receiver. Demux doesn't actually do any demultiplexing, but
   simply assigns the partition ID to be the same as the receive buffer's
   partition group, which is a job normally done by its namesake, the TupleDemux
 */
class Demux : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param stageName the name of the stage
   */
  Demux(uint64_t id, const std::string& stageName);

  /// Destructor
  virtual ~Demux() {}

  /**
     Assign the partition group ID as the partition ID for the buffer.

     \param buffer a buffer coming out of the receiver
   */
  void run(KVPairBuffer* buffer);
};

#endif // MIXEDIOBENCH_DEMUX_H
