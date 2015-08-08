#include "tests/themis_core/MultiDestinationWorker.h"

MultiDestinationWorker::MultiDestinationWorker(
  uint64_t id, const std::string& stageName)
  : SingleUnitRunnable(id, stageName) {
}

void MultiDestinationWorker::run(StringWorkUnit* workUnit) {
  const std::string& workUnitStr = workUnit->getString();

  std::map<std::string, uint64_t>::iterator iter = stringToTrackerIDMap.find(
    workUnitStr);

  if (iter != stringToTrackerIDMap.end()) {
    emitWorkUnit(iter->second, workUnit);
  } else {
    emitWorkUnit(workUnit);
  }
}

void MultiDestinationWorker::handleNamedDownstreamTracker(
  const std::string& trackerDescription, uint64_t trackerID) {
  stringToTrackerIDMap[trackerDescription] = trackerID;
}

BaseWorker* MultiDestinationWorker::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {
  return new MultiDestinationWorker(id, stageName);
}
