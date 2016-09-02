#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/KeyPartitionerInterface.h"
#include "mapreduce/functions/partition/BoundaryListPartitionFunction.h"

BoundaryListPartitionFunction::BoundaryListPartitionFunction(
  const KeyPartitionerInterface* _keyPartitioner)
  : keyPartitioner(_keyPartitioner) {
}

uint64_t BoundaryListPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  TRITONSORT_ASSERT(keyPartitioner != NULL, "Expected key partitioner to be non-null");
  return keyPartitioner->globalPartition(key, keyLength);
}

uint64_t BoundaryListPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  TRITONSORT_ASSERT(keyPartitioner != NULL, "Expected key partitioner to be non-null");
  return keyPartitioner->localPartition(key, keyLength, partitionGroup);
}

bool BoundaryListPartitionFunction::hashesKeys() const {
  return false;
}

bool BoundaryListPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {

  return filter.pass(key, keyLength);
}

uint64_t BoundaryListPartitionFunction::numGlobalPartitions() const {
  return keyPartitioner->numGlobalPartitions();
}
