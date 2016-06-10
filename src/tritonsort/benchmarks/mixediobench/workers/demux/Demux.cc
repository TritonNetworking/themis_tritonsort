#include "benchmarks/mixediobench/workers/demux/Demux.h"

Demux::Demux(uint64_t id, const std::string& stageName)
  : SingleUnitRunnable<KVPairBuffer>(id, stageName) {
}

void Demux::run(KVPairBuffer* buffer) {
  uint64_t partitionGroup = buffer->getPartitionGroup();
  // Benchmark assumes one partition group per partition, so just copy the group
  // ID as the partition ID.
  buffer->setLogicalDiskID(partitionGroup);

  emitWorkUnit(buffer);
}

BaseWorker* Demux::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  Demux* demux = new Demux(id, stageName);

  return demux;
}
