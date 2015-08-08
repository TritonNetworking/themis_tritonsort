#include "core/Params.h"
#include "mapreduce/functions/partition/UniformPartitionFunction.h"

UniformPartitionFunction::UniformPartitionFunction(
  const Params& params, uint64_t numPartitions)
  : globalPartitions(params.get<uint64_t>("NUM_PARTITION_GROUPS")),
    localPartitions(numPartitions) {
}

uint64_t UniformPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  // Scale the hash of the key by the number of nodes or physical disk
  // depending on the parameter.
  return (hash(key, keyLength) * globalPartitions) / MAX_PARTITIONS;
}

uint64_t UniformPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  // Scale the hash of the key by the number of logical disks, and ignore the
  // partition group hint.
  return (hash(key, keyLength) * localPartitions) / MAX_PARTITIONS;
}

bool UniformPartitionFunction::hashesKeys() const {
  return false;
}

bool UniformPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {

  return filter.pass(key, keyLength);
}

uint64_t UniformPartitionFunction::numGlobalPartitions() const {
  return globalPartitions;
}
