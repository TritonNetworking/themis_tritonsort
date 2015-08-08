#ifndef THEMIS_MULTI_QUEUE_RUNNABLE_H
#define THEMIS_MULTI_QUEUE_RUNNABLE_H

#include "core/BaseWorker.h"

/**
   MultiQueueRunnable is a special type of "pull-based" worker. Unlike
   BatchRunnable, the MultiQueueRunnable gets one new work unit at a time, but
   may pull a work unit from any input queue. This allows the user to make a
   distinction between input queues and worker threads, which is not possible
   with either SingleUnitRunnables or BatchRunnables.

   NOTE: MultiQueueRunnables are responsible for ensuring the type safety of
   their work units. Subclasses should dynamic cast items on the work queue to
   the expected type and abort if the cast fails.
 */
template <typename T> class MultiQueueRunnable : public BaseWorker {
public:
  /// Constructor
  /**
     \param id the worker's ID, unique among the workers in its stage

     \param typeName the worker's stage's name
   */
  MultiQueueRunnable(uint64_t id, const std::string& typeName);

  /// Perform this worker's task on all work units that it must process from its
  /// tracker.
  /**
     Processes work units from any of the tracker's queues. The worker is
     responsible for detecting when it will never receive another work unit.

     When this method returns, it is assumed that any work unit that will ever
     be processed by this worker has been processed.
   */
  virtual void run() = 0;

  /// \cond PRIVATE
  bool processIncomingWorkUnits();
  /// \endcond

protected:
  /**
     Get a new work unit from a particular queue. If the queue is empty, block
     until a work unit shows up.

     \param queueID the ID of the queue to pull from

     \return the work unit
   */
  T* getNewWork(uint64_t queueID);

  /**
     Try to get a new work unit from a particular queue. If no work is available
     in this queue, return false.

     \param queueID the ID of the queue to pull from

     \param[out] workUnit the work fetched from the queue, or NULL if no more
     work will ever be available from this queue

     \return true if work was fetched, and false if the queue is empty
  */
  bool attemptGetNewWork(uint64_t queueID, T*& workUnit);

  void startWaitForWorkTimer();

  void stopWaitForWorkTimer();

private:
  StatLogger logger;

  bool gotFirstWorkUnit;
};

template <typename T> MultiQueueRunnable<T>::MultiQueueRunnable(
  uint64_t id, const std::string& typeName)
  : BaseWorker(id, typeName),
    logger(typeName, id),
    gotFirstWorkUnit(false) {

  logWorkerType("pull");
}

template <typename T> T* MultiQueueRunnable<T>::getNewWork(uint64_t queueID) {
  Resource* resource = tracker->getNewWork(queueID);
  T* workUnit = NULL;

  if (resource != NULL) {
    workUnit = dynamic_cast<T*>(resource);
    ABORT_IF(workUnit == NULL,
             "Failed dynamic cast of non-NULL work unit. MultiQueueRunnable "
             "got the wrong type of work unit.");

    logConsumed(workUnit);

    if (!gotFirstWorkUnit) {
      gotFirstWorkUnit = true;
      BaseWorker::stopWaitForWorkTimer();
    }
  }

  return workUnit;
}

template <typename T> bool MultiQueueRunnable<T>::attemptGetNewWork(
  uint64_t queueID, T*& workUnit) {
  Resource* resource = NULL;
  bool gotNewWork = tracker->attemptGetNewWork(queueID, resource);

  if (gotNewWork && resource != NULL) {
    workUnit = dynamic_cast<T*>(resource);
    ABORT_IF(workUnit == NULL,
             "Failed dynamic cast of non-NULL work unit. MultiQueueRunnable "
             "got the wrong type of work unit.");

    logConsumed(workUnit);

    if (!gotFirstWorkUnit) {
      gotFirstWorkUnit = true;
      BaseWorker::stopWaitForWorkTimer();
    }
  } else {
    // Either got no work, or got a NULL work unit.
    workUnit = NULL;
  }

  return gotNewWork;
}

template <typename T> void MultiQueueRunnable<T>::startWaitForWorkTimer() {
  // To prevent inaccurate pipeline saturation logging, don't actually log the
  // wait unless we've fetched our first work unit.
  if (gotFirstWorkUnit) {
    BaseWorker::startWaitForWorkTimer();
  }
}

template <typename T> void MultiQueueRunnable<T>::stopWaitForWorkTimer() {
  if (gotFirstWorkUnit) {
    BaseWorker::stopWaitForWorkTimer();
  }
}

template <typename T> bool MultiQueueRunnable<T>::processIncomingWorkUnits() {
  // Begin in the state of waiting for work.
  BaseWorker::startWaitForWorkTimer();
  this->startRuntimeTimer();
  run();
  this->stopRuntimeTimer();
  this->logRuntimeInformation();
  this->resetRuntimeInformation();

  return true;
}

#endif // THEMIS_MULTI_QUEUE_RUNNABLE_H
