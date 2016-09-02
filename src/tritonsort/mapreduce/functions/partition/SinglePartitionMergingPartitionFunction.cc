#include "core/TritonSortAssert.h"
#include "mapreduce/functions/partition/SinglePartitionMergingPartitionFunction.h"

SinglePartitionMergingPartitionFunction::
SinglePartitionMergingPartitionFunction() {
}

uint64_t SinglePartitionMergingPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  return 0;
}

uint64_t SinglePartitionMergingPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  TRITONSORT_ASSERT(partitionGroup == 0, "Somehow ended up with non-zero partition group "
         "%llu, for single partition merging", partitionGroup);
  return 0;
}

bool SinglePartitionMergingPartitionFunction::hashesKeys() const {
  return false;
}

bool SinglePartitionMergingPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {

  return filter.pass(key, keyLength);
}

uint64_t SinglePartitionMergingPartitionFunction::numGlobalPartitions() const {
  // There's only one global partition.
  return 1;
}
