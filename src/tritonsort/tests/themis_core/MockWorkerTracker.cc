#include "MockWorkerTracker.h"
#include "core/Resource.h"
#include "core/TritonSortAssert.h"

void MockWorkerTracker::spawn() {
  // No-op
}

void MockWorkerTracker::addSource(WorkerTrackerInterface* prevTracker) {
  // No-op
}

bool MockWorkerTracker::hasAlreadySpawned() const {
  return true;
}

void MockWorkerTracker::noMoreWork() {
  // No-op
}

void MockWorkerTracker::notifyWorkerCompleted(uint64_t id) {
  // No-op
}

void MockWorkerTracker::createWorkers() {
  // No-op
}

void MockWorkerTracker::destroyWorkers() {
  // No-op
}

void MockWorkerTracker::waitForWorkersToFinish() {
  // No-op
}

void MockWorkerTracker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker) {
  downstreamTrackerVector.push_back(downstreamTracker);
}

void MockWorkerTracker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker,
  const std::string& trackerDescription) {

  ABORT("Not implemented");
}

const WorkerTrackerInterface::WorkerTrackerVector&
MockWorkerTracker::downstreamTrackers() const {
  return downstreamTrackerVector;
}

const std::string& MockWorkerTracker::getStageName() const {
  return stageName;
}

void MockWorkerTracker::getNewWork(uint64_t queueID, WorkQueue& destWorkQueue) {
  ABORT("Shouldn't be getting work from a MockWorkerTracker");
}

Resource* MockWorkerTracker::getNewWork(uint64_t queueID) {
  ABORT("Shouldn't be getting work from a MockWorkerTracker");
  return NULL;
}

bool MockWorkerTracker::attemptGetNewWork(
  uint64_t queueID, Resource*& workUnit) {
  ABORT("Shouldn't be getting work from a MockWorkerTracker");
  return false;
}

MockWorkerTracker::MockWorkerTracker(const std::string& _stageName)
  : stageName(_stageName) {
}

void MockWorkerTracker::addWorkUnit(Resource* workUnit) {
  workUnits.push(workUnit);
}

const std::queue<Resource*>& MockWorkerTracker::getWorkQueue() const {
  return workUnits;
}

void MockWorkerTracker::deleteAllWorkUnits() {
  while (!workUnits.empty()) {
    Resource* workUnit = workUnits.front();
    delete workUnit;
    workUnits.pop();
  }
}
