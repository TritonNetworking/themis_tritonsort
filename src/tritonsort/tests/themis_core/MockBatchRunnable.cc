#include "MockBatchRunnable.h"
#include "common/DummyWorkUnit.h"
#include "core/TritonSortAssert.h"

MockBatchRunnable::MockBatchRunnable(uint64_t id, const std::string& name)
  : BatchRunnable(id, name, 0) {
  workUnitsProcessed = 0;
}

void MockBatchRunnable::run(WorkQueue& workQueue) {
  while (!workQueue.willNotReceiveMoreWork() || !workQueue.empty()) {
    while (!workQueue.empty()) {
      DummyWorkUnit* workUnit = dynamic_cast<DummyWorkUnit*>(workQueue.front());
      workQueue.pop();
      ABORT_IF(workUnit == NULL, "Got NULL or incorrectly typed work unit.");
      delete workUnit;
      workUnitsProcessed++;
    }
    getNewWork();
  }
}

uint64_t MockBatchRunnable::getNumWorkUnitsProcessed() {
  return workUnitsProcessed;
}

BaseWorker* MockBatchRunnable::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  MockBatchRunnable* worker = new MockBatchRunnable(id, stageName);
  return worker;
}
