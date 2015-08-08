#include "Validator.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"

Validator::Validator(
  uint64_t id, const std::string& stageName,
  const SortStrategyFactory& stratFactory,
  uint64_t _allocatorCapacity, uint64_t& _scratchSize)
  : SingleUnitRunnable(id, stageName),
    allocatorCapacity(_allocatorCapacity),
    scratchSize(_scratchSize),
    ranOnce(false) {
  stratFactory.populateOrderedSortStrategyList(sortStrategies);

  ABORT_IF(sortStrategies.size() != 1,
           "Validator expects only 1 sort strategy, but got %llu",
           sortStrategies.size());
}

void Validator::run(KVPairBuffer* buffer) {
  ABORT_IF(ranOnce, "Should only get 1 work unit.");
  ranOnce = true;

  // Check to see if the scratch buffer size is feasiable.
  SortStrategyInterface* selectedStrategy = sortStrategies.front();

  // Store the scratch size
  scratchSize = selectedStrategy->getRequiredScratchBufferSize(buffer);

  // Make sure there will be enough memory so the system won't deadlock.
  uint64_t requiredAllocatorMemory =
    buffer->getCapacity() +    // Reader buffer
    scratchSize +              // Sorter scratch buffer
    buffer->getCurrentSize();  // Sorter output buffer

  // Only emit the buffer if the size is feasible.
  if (requiredAllocatorMemory <= allocatorCapacity) {
    emitWorkUnit(buffer);
  }
}

BaseWorker* Validator::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  SortStrategyFactory stratFactory(
    params.get<std::string>("SORT_STRATEGY"),
    params.get<bool>("USE_SECONDARY_KEYS"));

  uint64_t* scratchSize =
    dependencies.get<uint64_t>("scratch_size");

  Validator* validator = new Validator(
    id, stageName, stratFactory,
    params.get<uint64_t>("ALLOCATOR_CAPACITY"), *scratchSize);

  return validator;
}
