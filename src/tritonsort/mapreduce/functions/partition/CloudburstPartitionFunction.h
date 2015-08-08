#ifndef MAPRED_CLOUDBURST_PARTITION_FUNCTION_H
#define MAPRED_CLOUDBURST_PARTITION_FUNCTION_H

#include "HashedBoundaryListPartitionFunction.h"

/**
   The CloudBurstPartitionFunction operates like a
   HashedBoundaryListPartitionFunction with 1 difference: the last byte of the
   key is ignored for partitioning purposes. The last byte of a key in
   cloudburst contains metadata for grouping reference and query records
   corresponding to the same seed. Such records should be grouped into the same
   logical disk to allow the Reducer to operate properly.
 */
class CloudburstPartitionFunction : public HashedBoundaryListPartitionFunction {
public:
  /// Constructor
  /**
     \param keyPartitioner a phase 0 key partitioner to search for partitions
   */
  CloudburstPartitionFunction(const KeyPartitionerInterface* keyPartitioner);

  /**
     \sa PartitionFunctionInterface::globalPartition
   */
  uint64_t globalPartition(const uint8_t* key, uint32_t keyLength) const;

  /**
     \sa PartitionFunctionInterface::localPartition
   */
  uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const;

  bool acceptedByFilter(
    const uint8_t* key, uint32_t keyLength,
    const RecordFilter& filter) const;
};

#endif // MAPRED_CLOUDBURST_PARTITION_FUNCTION_H
