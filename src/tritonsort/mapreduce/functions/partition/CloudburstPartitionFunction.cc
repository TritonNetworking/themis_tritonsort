#include "CloudburstPartitionFunction.h"
#include "core/TritonSortAssert.h"

CloudburstPartitionFunction::CloudburstPartitionFunction(
  const KeyPartitionerInterface* keyPartitioner)
  : HashedBoundaryListPartitionFunction(keyPartitioner) {
}

uint64_t CloudburstPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  ABORT_IF(keyLength == 0, "Cannot global-hash 0-length Cloudburst key.");
  return HashedBoundaryListPartitionFunction::globalPartition(
    key, keyLength - 1);
}

uint64_t CloudburstPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  ABORT_IF(keyLength == 0, "Cannot local-hash 0-length Cloudburst key.");
  return HashedBoundaryListPartitionFunction::localPartition(
    key, keyLength - 1, partitionGroup);
}

bool CloudburstPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {

  ABORT_IF(keyLength == 0, "Cannot local-hash 0-length Cloudburst key.");
  return HashedBoundaryListPartitionFunction::acceptedByFilter(
    key, keyLength - 1, filter);
}
