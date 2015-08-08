#ifndef SORT_STRATEGY_BENCH_TIMESTAMPER_H
#define SORT_STRATEGY_BENCH_TIMESTAMPER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   Timestamper is a worker that records the time at which it receives an input
   buffer.

   The external timestamp variable is specified as a dependency named:
      stageName.timestamp
 */
class Timestamper : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker's id

     \param stageName the worker's name

     \param[out] timestamp the timestamper will store the timestamp of run() in
     this variable
   */
  Timestamper(uint64_t id, const std::string& stageName, uint64_t& timestamp);

  /**
     \sa SingleUnitRunnable::run

     Record the time at which the worker receives the buffer
   */
  void run(KVPairBuffer* buffer);

private:
  uint64_t& timestamp;
  bool ranOnce;
};

#endif // SORT_STRATEGY_BENCH_TIMESTAMPER_H
