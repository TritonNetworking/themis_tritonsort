#include "SelfStartingWorker.h"

SelfStartingWorker::SelfStartingWorker(
  uint64_t id, const std::string& stageName)
  : BaseWorker(id, stageName) {
  logWorkerType("self-starting");
}

SelfStartingWorker::~SelfStartingWorker() {
}

bool SelfStartingWorker::processIncomingWorkUnits() {
  // Self starting worker doesn't wait for work. So make sure our logging
  // infrastructure believes this worker has work from the moment this
  // function runs.
  startWaitForWorkTimer();
  stopWaitForWorkTimer();

  startRuntimeTimer();
  run();
  stopRuntimeTimer();

  logRuntimeInformation();

  return true;
}

