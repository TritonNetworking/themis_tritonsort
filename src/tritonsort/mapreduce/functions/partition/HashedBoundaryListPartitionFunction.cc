#include "HashedBoundaryListPartitionFunction.h"
#include "core/Hash.h"

HashedBoundaryListPartitionFunction::HashedBoundaryListPartitionFunction(
  const KeyPartitionerInterface* keyPartitioner)
  : BoundaryListPartitionFunction(keyPartitioner) {
}

uint64_t HashedBoundaryListPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  uint64_t hashedKey = Hash::hash(key, keyLength);
  return BoundaryListPartitionFunction::globalPartition(
    reinterpret_cast<uint8_t*>(&hashedKey), sizeof(uint64_t));
}

uint64_t HashedBoundaryListPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  uint64_t hashedKey = Hash::hash(key, keyLength);
  return BoundaryListPartitionFunction::localPartition(
    reinterpret_cast<uint8_t*>(&hashedKey), sizeof(uint64_t), partitionGroup);
}

bool HashedBoundaryListPartitionFunction::hashesKeys() const {
  return true;
}

bool HashedBoundaryListPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {
  uint64_t hashedKey = Hash::hash(key, keyLength);
  return filter.pass(
    reinterpret_cast<uint8_t*>(&hashedKey), sizeof(uint64_t));
}
