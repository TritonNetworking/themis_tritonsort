#ifndef TRITONSORT_TESTS_EMITTER_H
#define TRITONSORT_TESTS_EMITTER_H

#include <stdint.h>

#include "core/SelfStartingWorker.h"

class DummyWorkUnit;

// Emitter emits 'numWorkUnitsToEmit' count work units

class Emitter : public SelfStartingWorker {
WORKER_IMPL

public:
  Emitter(uint64_t id, const std::string& name, uint64_t numWorkUnitsToEmit);

  void run();

private:
  const uint64_t numWorkUnitsToEmit;
};

#endif // TRITONSORT_TESTS_EMITTER_H
