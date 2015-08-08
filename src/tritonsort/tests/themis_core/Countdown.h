#ifndef TRITONSORT_TESTS_COUNTDOWN_H
#define TRITONSORT_TESTS_COUNTDOWN_H

#include <stdint.h>

#include "core/SingleUnitRunnable.h"
#include "core/WorkerTrackerInterface.h"

class CountWorkUnit;

// Countdown tells its downstream tracker to spawn() when it receives a certain
// number of work units and emits the number of work units it received during
// the run at teardown

class Countdown : public SingleUnitRunnable<CountWorkUnit> {
WORKER_IMPL

public:
  Countdown(uint64_t id, const std::string& name, uint64_t countdownNumber,
            WorkerTrackerInterface& emitterTrackerToSpawn);
  void run(CountWorkUnit* workUnit);
  void teardown();

private:
  uint64_t countdown;
  uint64_t workUnitCount;

  WorkerTrackerInterface& emitterTrackerToSpawn;
};

#endif // TRITONSORT_TESTS_COUNTDOWN_H
