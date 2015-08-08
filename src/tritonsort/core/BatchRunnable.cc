#include "core/BatchRunnable.h"

BatchRunnable::BatchRunnable(
  uint64_t id, const std::string& typeName, uint64_t _maxInternalStateSize)
  : BaseWorker(id, typeName),
    internalStateSize(0),
    maxInternalStateSize(_maxInternalStateSize),
    gotFirstWorkUnit(false) {

  logWorkerType("pull");
}

BatchRunnable::~BatchRunnable() {
}

void BatchRunnable::getNewWork() {
  if (maxInternalStateSize == 0 || internalStateSize < maxInternalStateSize) {
    uint64_t oldSize = privateWorkUnitQueue.size();
    uint64_t oldTotalBytes = privateWorkUnitQueue.totalWorkSizeInBytes();
    tracker->getNewWork(getID(), privateWorkUnitQueue);
    uint64_t newTotalBytes = privateWorkUnitQueue.totalWorkSizeInBytes();
    uint64_t newSize = privateWorkUnitQueue.size();
    if (!gotFirstWorkUnit && newSize > 0) {
      // The time up until the first work unit will be counted as saturation
      // wait.
      gotFirstWorkUnit = true;
      BaseWorker::stopWaitForWorkTimer();
    }

    logConsumed(newSize - oldSize, newTotalBytes - oldTotalBytes);
  }
}

void BatchRunnable::startWaitForWorkTimer() {
  // To prevent inaccurate pipeline saturation logging, don't actually log the
  // wait unless we've fetched our first work unit.
  if (gotFirstWorkUnit) {
    BaseWorker::startWaitForWorkTimer();
  }
}

void BatchRunnable::stopWaitForWorkTimer() {
  if (gotFirstWorkUnit) {
    BaseWorker::stopWaitForWorkTimer();
  }
}

void BatchRunnable::resourceMonitorOutput(Json::Value& obj) {
  BaseWorker::resourceMonitorOutput(obj);
  obj["internal_state_size"] = Json::UInt64(internalStateSize);
  obj["max_internal_state_size"] = Json::UInt64(maxInternalStateSize);
}

StatLogger* BatchRunnable::registerIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger(getName(), getID());

  internalStateSizeStatID = intervalLogger->registerStat("internal_state_size");

  return intervalLogger;
}

void BatchRunnable::logIntervalStats(StatLogger& intervalLogger) const {
  intervalLogger.add(internalStateSizeStatID, internalStateSize);
}

bool BatchRunnable::processIncomingWorkUnits() {
  // Begin in the state of waiting for work.
  BaseWorker::startWaitForWorkTimer();
  startRuntimeTimer();
  run(privateWorkUnitQueue);
  stopRuntimeTimer();
  logRuntimeInformation();
  resetRuntimeInformation();

  return true;
}

