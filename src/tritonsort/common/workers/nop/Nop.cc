#include "common/workers/nop/Nop.h"

Nop::Nop(uint64_t id, const std::string& stageName)
  : SingleUnitRunnable<Resource>(id, stageName) {
}

void Nop::run(Resource* resource) {
  emitWorkUnit(resource);
}

BaseWorker* Nop::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  Nop* nop = new Nop(id, stageName);

  return nop;
}
