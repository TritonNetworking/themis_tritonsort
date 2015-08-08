#ifndef THEMIS_SELF_STARTING_WORKER_H
#define THEMIS_SELF_STARTING_WORKER_H

#include "core/BaseWorker.h"

/**
   Certain workers (typically synthetic workload generators) should run without
   expecting any external input. SelfStartingWorker provides a base class for
   this type of workers.
 */
class SelfStartingWorker : public BaseWorker {
public:
  /// Constructor
  /**
     \sa BaseWorker::BaseWorker
   */
  SelfStartingWorker(uint64_t id, const std::string& stageName);

  /// Destructor
  virtual ~SelfStartingWorker();

  /**
     Perform this worker's work. Self-starting workers expect to run exactly
     once.
   */
  virtual void run() = 0;

  /**
     Run this worker's run() method.

     \return true unconditionally, because this worker expects to run once.
   */
  bool processIncomingWorkUnits();
};

#endif // THEMIS_SELF_STARTING_WORKER_H
