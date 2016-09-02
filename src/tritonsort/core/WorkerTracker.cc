#include <limits.h>

#include "core/BaseWorker.h"
#include "core/MemoryUtils.h"
#include "core/ScopedLock.h"
#include "core/Timer.h"
#include "core/WorkQueueingPolicyFactory.h"
#include "core/WorkerTracker.h"

WorkerTracker::WorkerTracker(
  Params& params, const std::string& _phaseName, const std::string& _stageName,
  WorkQueueingPolicyFactory* workQueueingPolicyFactory)
  : logger(_stageName),
    phaseName(_phaseName),
    stageName(_stageName),
    spawned(false),
    source(false),
    teardownInitiated(false),
    teardownComplete(false),
    workerFactory(NULL),
    prevTrackers(0),
    completedPrevTrackers(0) {

  ResourceMonitor::registerClient(this, stageName.c_str());

  numWorkers = params.get<uint64_t>(
    "NUM_WORKERS." + phaseName + "." + stageName);

  implParamName = "WORKER_IMPLS." + phaseName + "." + stageName;

  pthread_mutex_init(&waitForWorkersLock, NULL);

  if (workQueueingPolicyFactory == NULL) {
    // No factory provided. Create a default factory and then create the policy.
    WorkQueueingPolicyFactory factory;
    workQueueingPolicy = factory.newWorkQueueingPolicy(
      phaseName, stageName, params);
  } else {
    // Use the specified factory to create the policy.
    workQueueingPolicy = workQueueingPolicyFactory->newWorkQueueingPolicy(
      phaseName, stageName, params);
  }
}

WorkerTracker::~WorkerTracker() {
  ResourceMonitor::unregisterClient(this);

  if (workQueueingPolicy != NULL) {
    delete workQueueingPolicy;
    workQueueingPolicy = NULL;
  }

  pthread_mutex_destroy(&waitForWorkersLock);
}

void WorkerTracker::resourceMonitorOutput(Json::Value& obj) {
  obj["type"] = "tracker";
  obj["spawned"] = spawned;
  obj["prev_trackers"] = Json::UInt64(prevTrackers);
  obj["completed_prev_trackers"] = Json::UInt64(completedPrevTrackers);
  obj["teardown_initiated"] = teardownInitiated;
  obj["teardown_complete"] = teardownComplete;

  if (downstreamTrackerVector.size() > 0) {
    obj["next_stages"] = Json::Value(Json::arrayValue);

    for (WorkerTrackerVector::iterator iter = downstreamTrackerVector.begin();
         iter != downstreamTrackerVector.end(); iter++) {
      obj["next_stages"].append((*iter)->getStageName());
    }
  }
}

uint64_t WorkerTracker::addDownstreamTrackerReturningID(
  WorkerTrackerInterface* downstreamTracker) {

  uint64_t trackerID = downstreamTrackerVector.size();
  downstreamTrackerVector.push_back(downstreamTracker);
  downstreamTracker->addSource(this);

  return trackerID;
}

void WorkerTracker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker) {
  // Add the tracker in the usual way, but ignore the ID it returns
  addDownstreamTrackerReturningID(downstreamTracker);
}

void WorkerTracker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker,
  const std::string& trackerDescription) {
  // Make sure that a different tracker wasn't registered with the same name
  ABORT_IF(downstreamTrackerDescriptions.count(trackerDescription) == 1,
           "Already added a downstream tracker with description '%s' to this "
           "tracker", trackerDescription.c_str());

  downstreamTrackerDescriptions.insert(trackerDescription);

  uint64_t downstreamTrackerID = addDownstreamTrackerReturningID(
    downstreamTracker);

  downstreamTrackerIDToName[downstreamTrackerID] = trackerDescription;
}

const WorkerTrackerInterface::WorkerTrackerVector&
WorkerTracker::downstreamTrackers() const {
  return downstreamTrackerVector;
}

uint64_t WorkerTracker::getNumWorkers() const {
  return numWorkers;
}

void WorkerTracker::addWorkUnit(Resource* workUnit) {
  if (workUnit == NULL) {
    // Got "no more work" signal at some point in the past from one of
    // our upstream trackers

    completedPrevTrackers++;

    TRITONSORT_ASSERT(source || completedPrevTrackers <= prevTrackers,
           "%s: Got more than one \"no more work\" signal from a "
           "previous tracker (%llu trackers, %llu complete trackers)",
           stageName.c_str(), prevTrackers, completedPrevTrackers);

    if (source || completedPrevTrackers == prevTrackers) {
      // If we've received a "no more work" signal from all previous trackers,
      // we're ready to teardown

      teardownInitiated = true;

      workQueueingPolicy->teardown();
    }
  } else {
    workQueueingPolicy->enqueue(workUnit);
  }
}

void WorkerTracker::setFactory(WorkerFactory* workerFactory,
                               const std::string& applicationName,
                               const std::string& workerTypeName) {
  this->workerFactory = workerFactory;
  this->applicationName.assign(applicationName);
  this->workerTypeName.assign(workerTypeName);
}

void WorkerTracker::setFactory(WorkerFactory* workerFactory,
                               const std::string& workerTypeName) {
  this->workerFactory = workerFactory;
  this->workerTypeName.assign(workerTypeName);
}

void WorkerTracker::notifyWorkerCompleted(uint64_t id) {
  pthread_mutex_lock(&waitForWorkersLock);

  BaseWorker* worker = workers[id];

  if (completedWorkerSet.count(worker) == 0) {
    completedWorkerSet.insert(worker);
  }

  // If all workers have finished, then this stage is done
  if (completedWorkerSet.size() == numWorkers) {
    // Tell the downstream trackers that they won't get more work

    for (WorkerTrackerVector::iterator iter = downstreamTrackerVector.begin();
         iter != downstreamTrackerVector.end(); iter++) {
      (*iter)->noMoreWork();
    }

    teardownComplete = true;

    // Signal main() that this stage is done executing
    barrier.signal();

    // Log the total runtime for this stage.
    stageRuntimeTimer.stop();
    logger.logDatum("stage_runtime", stageRuntimeTimer);
  }
  pthread_mutex_unlock(&waitForWorkersLock);
}

void WorkerTracker::waitForWorkersToFinish() {
  // Wait until the last worker completes its work.
  barrier.wait();

  // Join all worker threads.
  workers.beginThreadSafeIterate();

  for (WorkerVector::iterator iter = workers.begin(); iter != workers.end();
       iter++) {
    // Even though this method technically can wait for the worker (by joining
    // its thread), all workers will have finished by the time we reach this
    // point.
    (*iter)->waitForWorkerToFinish();
  }

  workers.endThreadSafeIterate();
}

void WorkerTracker::createWorkers() {
  ABORT_IF(workerFactory == NULL, "Must set a factory for this tracker");

  logger.logDatum("num_workers", numWorkers);

  for (uint64_t i = 0; i < numWorkers; i++) {
    BaseWorker* worker = NULL;

    try {
      if (applicationName.size() == 0) {
        worker = workerFactory->newInstance(
          workerTypeName, implParamName, phaseName, stageName, i);
      } else {
        worker = workerFactory->newInstance(
          applicationName, workerTypeName, implParamName, phaseName, stageName,
          i);
      }

    } catch (std::bad_alloc exception) {
      ABORT("Ran out of memory while allocating workers");
    }

    worker->setTracker(this);
    workers.push(worker);
  }
}

void WorkerTracker::destroyWorkers() {
  workers.beginThreadSafeIterate();
  for (WorkerVector::iterator iter = workers.begin(); iter != workers.end();
       iter++) {
    BaseWorker* worker = *iter;
    if (worker != NULL) {
      delete worker;
    }
  }
  workers.endThreadSafeIterate();
  workers.clear();

  completedWorkerSet.clear();
  barrier.reset();

  spawned = false;
  teardownInitiated = false;
  teardownComplete = false;
  completedPrevTrackers = 0;
}

void WorkerTracker::spawn() {
  stageRuntimeTimer.start();

  spawned = true;

  for (WorkerTrackerVector::iterator iter = downstreamTrackerVector.begin();
       iter != downstreamTrackerVector.end(); iter++) {
    WorkerTrackerInterface* downstreamTracker = *iter;

    if (!downstreamTracker->hasAlreadySpawned() && downstreamTracker != this) {
      downstreamTracker->spawn();
    }
  }

  if (source) {
    // Source will auto-complete when its work queue is empty.
    noMoreWork();
  }

  spawnWorkers();
}

void WorkerTracker::noMoreWork() {
  addWorkUnit(NULL);
}

void WorkerTracker::isSourceTracker() {
  source = true;
}

bool WorkerTracker::hasAlreadySpawned() const {
  return spawned;
}

WorkerTracker::WorkerVector& WorkerTracker::getWorkers() {
  return workers;
}

void WorkerTracker::addSource(WorkerTrackerInterface* prevTracker) {
  prevTrackers++;
}

const std::string& WorkerTracker::getStageName() const {
  return stageName;
}

void WorkerTracker::getNewWork(uint64_t queueID, WorkQueue& destWorkUnitQueue) {
  workQueueingPolicy->batchDequeue(queueID, destWorkUnitQueue);
}

Resource* WorkerTracker::getNewWork(uint64_t queueID) {
  return workQueueingPolicy->dequeue(queueID);
}

bool WorkerTracker::attemptGetNewWork(uint64_t queueID, Resource*& workUnit) {
  return workQueueingPolicy->nonBlockingDequeue(queueID, workUnit);
}

void WorkerTracker::spawnWorkers() {
  workers.beginThreadSafeIterate();
  for (WorkerVector::iterator workerIter = workers.begin();
       workerIter != workers.end(); workerIter++) {
    BaseWorker* worker = *workerIter;

    for (uint64_t trackerID = 0; trackerID < downstreamTrackerVector.size();
         trackerID++) {

      WorkerTrackerInterface* downstreamTracker =
        downstreamTrackerVector[trackerID];

      std::map<uint64_t, std::string>::iterator trackerNameIter =
        downstreamTrackerIDToName.find(trackerID);

      if (trackerNameIter != downstreamTrackerIDToName.end()) {
        worker->addDownstreamTracker(
          downstreamTracker, trackerNameIter->second);
      } else {
        worker->addDownstreamTracker(downstreamTracker);
      }
    }

    worker->spawn();
  }
  workers.endThreadSafeIterate();

  spawned = true;
}
