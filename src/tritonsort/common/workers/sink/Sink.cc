#include "Sink.h"

Sink::Sink(uint64_t id, const std::string& stageName)
  : SingleUnitRunnable(id, stageName) {
}

void Sink::run(Resource* resource) {
  if (resource != NULL) {
    delete resource;
  }
}

BaseWorker* Sink::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  Sink* sink = new Sink(id, stageName);

  return sink;
}
