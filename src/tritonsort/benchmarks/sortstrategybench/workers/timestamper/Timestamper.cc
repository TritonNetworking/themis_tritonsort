#include "Timestamper.h"
#include "core/Timer.h"

Timestamper::Timestamper(
  uint64_t id, const std::string& stageName, uint64_t& _timestamp)
  : SingleUnitRunnable(id, stageName),
    timestamp(_timestamp),
    ranOnce(false) {
}

void Timestamper::run(KVPairBuffer* buffer) {
  ABORT_IF(ranOnce, "Should only get 1 work unit.");
  ranOnce = true;

  // Record the timestamp.
  timestamp = Timer::posixTimeInMicros();

  // Forward the buffer downstream.
  emitWorkUnit(buffer);
}

BaseWorker* Timestamper::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t* timestamp = dependencies.get<uint64_t>(stageName, "timestamp");

  Timestamper* timestamper = new Timestamper(id, stageName, *timestamp);

  return timestamper;
}
