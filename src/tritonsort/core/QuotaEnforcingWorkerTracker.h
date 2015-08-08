#ifndef THEMIS_QUOTA_ENFORCING_WORKER_TRACKER_H
#define THEMIS_QUOTA_ENFORCING_WORKER_TRACKER_H

#include "core/WorkerTracker.h"

class MemoryQuota;

class QuotaEnforcingWorkerTracker : public WorkerTracker {
public:
  QuotaEnforcingWorkerTracker(
    Params& params, const std::string& phaseName, const std::string& stageName,
    WorkQueueingPolicyFactory* workQueueingPolicyFactory = NULL);

  void addProducerQuota(MemoryQuota& quota);
  void addConsumerQuota(MemoryQuota& quota);

  void addWorkUnit(Resource* workUnit);
  void getNewWork(uint64_t queueID, WorkQueue& destWorkUnitQueue);
  Resource* getNewWork(uint64_t queueID);
  bool attemptGetNewWork(uint64_t queueID, Resource*& workUnit);
private:
  MemoryQuota* producerQuota;
  MemoryQuota* consumerQuota;
};

#endif // THEMIS_QUOTA_ENFORCING_WORKER_TRACKER_H
