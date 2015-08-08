#ifndef TRITONSORT_WORKER_TRACKER_INTERFACE_H
#define TRITONSORT_WORKER_TRACKER_INTERFACE_H

#include <stdint.h>
#include <string>
#include <vector>

class Resource;
class WorkQueue;

/**
   WorkerTrackerInterface is a partial interface to WorkerTracker that can be
   used as a nextTracker in place of a true WorkerTracker. This allows the unit
   tests to mock out a worker tracker that doesn't actually run a thread, but
   rather lets the test harness check for the correct work units.
 */
class WorkerTrackerInterface {
public:
  typedef std::vector<WorkerTrackerInterface*> WorkerTrackerVector;

  /// Spawn the worker tracker's monitor thread
  virtual void spawn() = 0;

  /**
     "Downstream" trackers are trackers for which this tracker's workers will
     produce work.

     This signature for the addDownstreamTracker method is used in the common
     case, where a tracker has only one downstream tracker and will not need to
     programmatically route work to one tracker or another.

     \param downstreamTracker a tracker that is a downstream tracker for this
     one
   */
  virtual void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker) = 0;

  /**
     "Downstream" trackers are trackers for which this tracker's workers will
     produce work.

     This signature for the addDownstreamTracker method is used when workers
     need to programmatically route work to different trackers based on the
     work unit being routed.

     When adding a downstream tracker using this method, the caller specifies a
     descriptive name which will be passed to the worker's addDownstreamTracker
     method so that the worker can figure out the trackers to which its
     different kinds of work should be routed.

     \param downstreamTracker a tracker that is a downstream tracker for this
     one

     \param trackerDescription a descriptive name for the downstream tracker
     relative to this one, e.g. "red, square Foos" or "localhost bypass"
   */
  virtual void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker,
    const std::string& trackerDescription) = 0;

  /**
     \return a list of trackers that are downstream from this one
   */
  virtual const WorkerTrackerVector& downstreamTrackers()
    const = 0;

  /**
      Note that this tracker has an upstream tracker connected to it; must be
      called by all upstream stage trackers or whatever else you're connecting
      to it to ensure that the stage graph structure is sane

      \param prevTracker a pointer to the upstream tracker
   */
  virtual void addSource(WorkerTrackerInterface* prevTracker) = 0;

  /**
     Receive a work unit from the upstream stage

     \param workUnit the work unit being received
   */
  virtual void addWorkUnit(Resource* workUnit) = 0;

  /// Check if the tracker has already had WorkerTracker::spawn called on it;
  /// ensures that trackers aren't spawned more than once
  virtual bool hasAlreadySpawned() const = 0;

  /// Notify this tracker that it will not receive more work units
  virtual void noMoreWork() = 0;

  /// As a worker, notify your tracker that you have finished
  /**
     \param id the worker ID of the notifying worker
   */
  virtual void notifyWorkerCompleted(uint64_t id) = 0;

  /// Create workers for this stage
  virtual void createWorkers() = 0;

  /// Destroy all workers for this stage
  /**
     Assuming that workers have been previously created by
     WorkerTrackerInterface::createWorkers, delete them. This should only be
     called after workers have completed teardown.
   */
  virtual void destroyWorkers() = 0;

  /// Wait for all workers to finish. Returns only when all workers have
  /// completed teardown.
  virtual void waitForWorkersToFinish() = 0;

  /**
     \return the stage name corresponding to this tracker
  */
  virtual const std::string& getStageName() const = 0;

  /// Give the requesting worker as much work as possible from a work queue
  /**
     \param queueID the unique ID associated with the queue. In the common case
     this will be the worker ID

     \param[out] destWorkUnitQueue the work unit queue into which to move any
     work that you're giving this worker
   */
  virtual void getNewWork(uint64_t queueID, WorkQueue& destWorkUnitQueue) = 0;

  /// Give the requesting worker a work unit from a work queue
  /**
     \param queueID the unique ID associated with the queue. In the common case
     this will be the worker ID

     \return a pointer to the requested resource, or NULL if no further
     resources will be received
   */
  virtual Resource* getNewWork(uint64_t queueID) = 0;

  /// Give the requesting worker a work unit from a work queue if the queue is
  /// non-empty.
  /**
     \param queueID the unique ID associated with the queue

     \param[out] workUnit the next work unit in the queue, or NULL if no further
     resources will be received in this queue

     \return true if the queue had work, and false if the queue was empty
   */
  virtual bool attemptGetNewWork(uint64_t queueID, Resource*& workUnit) = 0;
};

#endif // TRITONSORT_WORKER_TRACKER_INTERFACE_H
