#ifndef TRITONSORT_BATCH_RUNNABLE_H
#define TRITONSORT_BATCH_RUNNABLE_H

#include "core/BaseWorker.h"
#include "core/Timer.h"

/**
   BatchRunnable is our canonical "pull-based" worker. It processes its work
   units in a batch, explicitly pulling more work units as it goes rather than
   waiting for them to arrive and processing them one at a time, as
   SingleUnitRunnable does.

   NOTE: BatchRunnables are responsible for ensuring the type safety of their
   work units. Subclasses should dynamic cast items on the work queue to the
   expected type and abort if the cast fails.
 */

class BatchRunnable : public BaseWorker {
public:
  /// Constructor
  /**
     \param id the worker's ID, unique among the workers in its stage

     \param name the worker's stage's name

     \param maxInternalStateSize the maximum size in bytes that this worker's
     internal state can have before calls to getNewWork() don't dequeue work
     anymore. Size is tracked by subclasses through modifying the
     internalStateSize field.
   */
  BatchRunnable(
    uint64_t id, const std::string& typeName, uint64_t maxInternalStateSize);

  /// Destructor
  virtual ~BatchRunnable();

  /// Perform this worker's task on the work units in its work queue
  /**
     Processes the work units in the worker's work queue. Management of the
     work queue and detecting when it will never receive another work unit is
     the worker's responsibility.

     When this method returns, it is assumed that any work unit that will ever
     be processed by this worker has been processed.

     \param workQueue a reference to the worker's private work queue
   */
  virtual void run(WorkQueue& workQueue) = 0;

  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
  */
  virtual void resourceMonitorOutput(Json::Value& obj);

  virtual StatLogger* registerIntervalStatLogger();

  virtual void logIntervalStats(StatLogger& logger) const;

  /// \cond PRIVATE
  bool processIncomingWorkUnits();
  /// \endcond

protected:
  void getNewWork();

  void startWaitForWorkTimer();

  void stopWaitForWorkTimer();

  uint64_t internalStateSize;
private:
  const uint64_t maxInternalStateSize;
  WorkQueue privateWorkUnitQueue;

  bool gotFirstWorkUnit;

  uint64_t internalStateSizeStatID;
};

#endif // TRITONSORT_BATCH_RUNNABLE_H
