#include "core/Resource.h"
#include "core/TritonSortAssert.h"
#include "tests/themis_core/ManualConsumerWorker.h"

ManualConsumerWorker::ManualConsumerWorker(
  const std::string& stageName, uint64_t id)
  : BaseWorker(id, stageName) {
}

ManualConsumerWorker::~ManualConsumerWorker() {
}

void ManualConsumerWorker::clearWorkUnitQueue() {
  while (!workUnitQueue.empty()) {
    Resource* workUnit = workUnitQueue.front();
    workUnitQueue.pop();
    delete workUnit;
  }
}

void ManualConsumerWorker::setNumWorkUnitsConsumed(uint64_t workUnitsConsumed) {
  logConsumed(workUnitsConsumed, 0);
}

void ManualConsumerWorker::getAllWorkFromTracker() {
  tracker->getNewWork(getID(), workUnitQueue);
  logConsumed(workUnitQueue.size(), 0);
}

uint64_t ManualConsumerWorker::workUnitQueueSize() {
  return workUnitQueue.size();
}

bool ManualConsumerWorker::processIncomingWorkUnits() {
  ABORT("Shouldn't be actually executing this worker; it's designed for manual "
        "control");
  return true;
}

BaseWorker* ManualConsumerWorker::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {
  return new ManualConsumerWorker(stageName, id);
}
