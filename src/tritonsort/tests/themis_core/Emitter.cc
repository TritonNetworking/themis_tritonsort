#include "CountWorkUnit.h"
#include "Emitter.h"
#include "common/DummyWorkUnit.h"

Emitter::Emitter(uint64_t id, const std::string& name,
                 uint64_t _numWorkUnitsToEmit)
  : SelfStartingWorker(id, name),
    numWorkUnitsToEmit(_numWorkUnitsToEmit) {
}

void Emitter::run() {
  for (uint64_t i = 0; i < numWorkUnitsToEmit; i++) {
    emitWorkUnit(new CountWorkUnit(1));
  }
}

BaseWorker* Emitter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t numWorkUnitsToEmit = params.get<uint64_t>(
    "EMITTER_WORK_UNIT_COUNT");

  Emitter* emitter = new Emitter(id, stageName, numWorkUnitsToEmit);
  return emitter;
}
