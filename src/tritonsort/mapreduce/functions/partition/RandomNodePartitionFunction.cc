#include <stdlib.h>

#include "core/TritonSortAssert.h"
#include "mapreduce/functions/partition/RandomNodePartitionFunction.h"

RandomNodePartitionFunction::RandomNodePartitionFunction(uint64_t _numNodes)
  : numNodes(_numNodes) {
}

uint64_t RandomNodePartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  return lrand48() % numNodes;
}

uint64_t RandomNodePartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  TRITONSORT_ASSERT(partitionGroup == 0, "Somehow ended up with non-zero partition group "
         "%llu, for random node", partitionGroup);
  return 0;
}

bool RandomNodePartitionFunction::hashesKeys() const {
  return false;
}

bool RandomNodePartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {

  return filter.pass(key, keyLength);
}

uint64_t RandomNodePartitionFunction::numGlobalPartitions() const {
  return numNodes;
}
