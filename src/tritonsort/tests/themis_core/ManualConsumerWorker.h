#ifndef THEMIS_MANUAL_CONSUMER_WORKER_H
#define THEMIS_MANUAL_CONSUMER_WORKER_H

#include "core/BaseWorker.h"
#include "core/WorkQueue.h"

class ManualConsumerWorker : public BaseWorker {
WORKER_IMPL

public:
  ManualConsumerWorker(const std::string& stageName, uint64_t id);
  virtual ~ManualConsumerWorker();

  void setNumWorkUnitsConsumed(uint64_t workUnitsConsumed);
  void getAllWorkFromTracker();

  bool processIncomingWorkUnits();
  void clearWorkUnitQueue();

  uint64_t workUnitQueueSize();
private:
  WorkQueue workUnitQueue;
};

#endif // THEMIS_MANUAL_CONSUMER_WORKER_H
