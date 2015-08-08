#include "core/MemoryQuota.h"
#include "core/QuotaEnforcingWorkerTracker.h"
#include "core/Resource.h"

QuotaEnforcingWorkerTracker::QuotaEnforcingWorkerTracker(
  Params& params, const std::string& phaseName, const std::string& stageName,
  WorkQueueingPolicyFactory* workQueueingPolicyFactory)
  : WorkerTracker(params, phaseName, stageName, workQueueingPolicyFactory),
    producerQuota(NULL),
    consumerQuota(NULL) {
}

void QuotaEnforcingWorkerTracker::addProducerQuota(MemoryQuota& quota) {
  producerQuota = &quota;
}

void QuotaEnforcingWorkerTracker::addConsumerQuota(MemoryQuota& quota) {
  consumerQuota = &quota;
}

void QuotaEnforcingWorkerTracker::addWorkUnit(Resource* workUnit) {
  if (workUnit != NULL && producerQuota != NULL) {
    producerQuota->addUsage(workUnit->getCurrentSize());
  }

  WorkerTracker::addWorkUnit(workUnit);
}

void QuotaEnforcingWorkerTracker::getNewWork(
  uint64_t queueID, WorkQueue& destWorkUnitQueue) {

  uint64_t oldSize = destWorkUnitQueue.totalWorkSizeInBytes();

  WorkerTracker::getNewWork(queueID, destWorkUnitQueue);

  if (consumerQuota != NULL) {
    consumerQuota->removeUsage(
      destWorkUnitQueue.totalWorkSizeInBytes() - oldSize);
  }
}

Resource* QuotaEnforcingWorkerTracker::getNewWork(uint64_t queueID) {
  Resource* resource = WorkerTracker::getNewWork(queueID);

  if (resource != NULL && consumerQuota != NULL) {
    consumerQuota->removeUsage(resource->getCurrentSize());
  }

  return resource;
}

bool QuotaEnforcingWorkerTracker::attemptGetNewWork(
  uint64_t queueID, Resource*& workUnit) {

  bool attemptSucceeded = WorkerTracker::attemptGetNewWork(queueID, workUnit);

  if (workUnit != NULL && consumerQuota != NULL) {
    consumerQuota->removeUsage(workUnit->getCurrentSize());
  }

  return attemptSucceeded;
}
