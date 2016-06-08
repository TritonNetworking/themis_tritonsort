#ifndef _TRITONSORT_WORKER_TRACKER_H
#define _TRITONSORT_WORKER_TRACKER_H

#include <iostream>
#include <pthread.h>
#include <queue>
#include <set>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>

#include "tritonsort/config.h"
#include "core/Params.h"
#include "core/ResourceMonitor.h"
#include "core/ResourceMonitorClient.h"
#include "core/StatusPrinter.h"
#include "core/ThreadSafeVector.h"
#include "core/ThreadSafeWorkQueue.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/WorkQueueingPolicyInterface.h"
#include "core/WorkerBarrier.h"
#include "core/WorkerFactory.h"
#include "core/WorkerTracker.h"
#include "core/WorkerTrackerInterface.h"
#include "core/constants.h"

class BaseWorker;
class Params;
class Resource;
class WorkQueueingPolicyFactory;

/**
   There is a single tracker per stage that is responsible for managing the
   workers in that stage. It is also responsible for passing work from workers
   in downstream stages to workers in its stage.

   Trackers are no longer parameterized on input and output types, so the user
   must ensure type safety via dynamic casts in the workers. SingleUnitRunnable
   performs the type safety automatically, but BatchRunnables must manually
   check each element in the work queue.
 */
class WorkerTracker
  : public WorkerTrackerInterface, public ResourceMonitorClient {
public:
  typedef ThreadSafeVector<BaseWorker*> WorkerVector;
  typedef ThreadSafeVector<ThreadSafeWorkQueue> WorkQueueVector;

  /// Constructor
  /**
     All trackers are automatically registered with the ResourceMonitor at
     construction time.

     The constructor is configured by the following parameters:

     NUM_WORKERS.<phaseName>.<stageName>: the number of workers spawned by this
     tracker

     WORK_QUEUE_SATURATION_SIZE.<phaseName>.<stageName> (optional, defaults to
     UINT64_MAX): if the number of work units in worker queues exceeds this
     value, any calls to addWorkUnit will block until the number of work units
     in worker queues falls below the desaturation size (see below).

     WORK_QUEUE_DESATURATION_SIZE.<phaseName>.<stageName> (optional, defaults
     to work queue saturation size): if the number of work units in worker
     queues falls below this value, any blocked calls to addWorkUnit will
     unblock

     \param params a Params object that will be used to configure this tracker

     \param phaseName the name of the phase in which the tracker is running

     \param stageName the name of the stage that the tracker is managing

     \param workQueueingPolicyFactory a factory that will create a queueing
     policy for this tracker, or NULL if the default factory should be used
   */
  WorkerTracker(
    Params& params, const std::string& phaseName, const std::string& stageName,
    WorkQueueingPolicyFactory* workQueueingPolicyFactory = NULL);

  /// Destructor
  virtual ~WorkerTracker();

  /// \sa ResourceMonitorClient::resourceMonitorOutput
  void resourceMonitorOutput(Json::Value& obj);

  /// \return the number of queues in this tracker
  uint64_t getNumQueues();

  void addDownstreamTracker(WorkerTrackerInterface* downstreamTracker);

  void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker,
    const std::string& trackerDescription);

  const std::vector<WorkerTrackerInterface*>& downstreamTrackers() const;

  /// Get the number of worker instances in this stage
  /**
     \return the number of workers for this stage
   */
  uint64_t getNumWorkers() const;

  /// Receive a work unit from the upstream stage
  /**
     \sa WorkerTrackerInterface::addWorkUnit
   */
  virtual void addWorkUnit(Resource* workUnit);

  /// Set the worker factory used for creating workers
  /**
     \todo (AR) once migration to new-style workers is complete, make
     application type, worker type, factory, etc construction parameters for
     WorkerTracker

     \param workerFactory the factory to use

     \param applicationName the application under which the desired worker is
     registered

     \param workerTypeName the desired worker's registered worker type
   */
  void setFactory(
    WorkerFactory* workerFactory, const std::string& applicationName,
    const std::string& workerTypeName);

  /// Set the worker factory used for creating workers
  /**
     \todo (AR) once migration to new-style workers is complete, make
     application type, worker type, factory, etc construction parameters for
     WorkerTracker

     \param workerFactory the factory to use

     \param workerTypeName the desired worker's registered worker type
  */
  void setFactory(
    WorkerFactory* workerFactory, const std::string& workerTypeName);

  /// As a worker, notify your tracker that you have finished
  /**
     \sa WorkerTrackerInterface::notifyWorkerCompleted
   */
  virtual void notifyWorkerCompleted(uint64_t id);

  /// Wait for all workers to finish
  /**
     \sa WorkerTrackerInterface::waitForWorkersToFinish
  */
  virtual void waitForWorkersToFinish();

  /// Create workers for this stage
  /**
     \sa WorkerTrackerInterface::createWorkers
   */
  virtual void createWorkers();

  /// Destroy all workers for this stage
  /**
     \sa WorkerTrackerInterface::destroyWorkers
   */
  virtual void destroyWorkers();

  /// Spawn the worker tracker's monitor thread
  /**
     \sa WorkerTrackerInterface::spawn
   */
  virtual void spawn();

  /// Notify this tracker that it will not receive more work units
  /**
     \sa WorkerTrackerInterface::spawn
   */
  void noMoreWork();

  /// Notify this tracker that it is a source tracker
  void isSourceTracker();

  /// Check if the tracker has already had WorkerTracker::spawn called on it;
  /// ensures that trackers aren't spawned more than once
  /**
     \sa WorkerTrackerInterface::hasAlreadySpawned
   */
  virtual bool hasAlreadySpawned() const;

  /// Get a list of workers managed by this tracker
  /**
     \todo (AR) this really shouldn't be necessary and should probably be
     removed

     \return a reference to the tracker's workers
   */
  WorkerVector& getWorkers();

  /// Called by an upstream tracker to connect this one to it
  /**
     \sa WorkerTrackerInterface::addSource
   */
  virtual void addSource(WorkerTrackerInterface* prevTracker);

  /// \sa WorkerTrackerInterface::getStageName
  virtual const std::string& getStageName() const;

  /// \sa WorkerTrackerInterface::getNewWork
  virtual void getNewWork(uint64_t queueID, WorkQueue& destWorkUnitQueue);

  virtual Resource* getNewWork(uint64_t queueID);

  /// \sa WorkerTrackerInterface::attemptGetNewWork
  virtual bool attemptGetNewWork(uint64_t queueID, Resource*& workUnit);

private:
  /**
     Spawn all worker threads managed by this tracker.
   */
  void spawnWorkers();

  uint64_t addDownstreamTrackerReturningID(
    WorkerTrackerInterface* downstreamTracker);

  Timer stageRuntimeTimer;

  StatLogger logger;

  WorkerTrackerVector downstreamTrackerVector;
  std::map<uint64_t, std::string> downstreamTrackerIDToName;
  std::set<std::string> downstreamTrackerDescriptions;

  std::string phaseName;
  std::string stageName;

  bool spawned;

  pthread_t trackerThreadID;

  // This tracker's stage does not have any incoming stages, and should
  // terminate as soon as the last of its initial inputs are processed.
  bool source;

  // The workers in this stage have begun to execute teardown.
  bool teardownInitiated;
  // All the workers in this stage have completed teardown.
  bool teardownComplete;

  WorkerBarrier barrier;

  pthread_mutex_t waitForWorkersLock;
  std::set< BaseWorker* > completedWorkerSet;

  WorkerVector workers;

  WorkQueueingPolicyInterface* workQueueingPolicy;

  uint64_t numWorkers;

  WorkerFactory* workerFactory;

  std::string applicationName;
  std::string workerTypeName;
  std::string implParamName;

  uint64_t prevTrackers;
  uint64_t completedPrevTrackers;
};

#endif //_TRITONSORT_WORKER_TRACKER_H
