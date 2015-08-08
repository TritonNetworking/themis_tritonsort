#include "CountWorkUnit.h"
#include "Countdown.h"
#include "common/DummyWorkUnit.h"

Countdown::Countdown(
  uint64_t id, const std::string& name, uint64_t countdownNumber,
  WorkerTrackerInterface& _emitterTrackerToSpawn)
  : SingleUnitRunnable<CountWorkUnit>(id, name),
    emitterTrackerToSpawn(_emitterTrackerToSpawn) {
  countdown = countdownNumber;
  workUnitCount = 0;
}

void Countdown::run(CountWorkUnit* workUnit) {
  countdown--;
  workUnitCount++;

  if (countdown == 0) {
    emitterTrackerToSpawn.spawn();
  }

  delete workUnit;
}

void Countdown::teardown() {
  emitWorkUnit(new CountWorkUnit(workUnitCount));
}

BaseWorker* Countdown::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  WorkerTrackerInterface* trackerToSpawn =
    dependencies.get<WorkerTrackerInterface>("tracker_to_spawn");

  uint64_t countdownNumber = params.get<uint64_t>("COUNTDOWN_NUMBER");

  Countdown* countdown = new Countdown(id, stageName, countdownNumber,
                                       *trackerToSpawn);

  return countdown;
}
