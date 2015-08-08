#ifndef MAPRED_HASHED_BOUNDARY_LIST_PARTITION_FUNCTION_H
#define MAPRED_HASHED_BOUNDARY_LIST_PARTITION_FUNCTION_H

#include "BoundaryListPartitionFunction.h"

/**
   A HashedBoundaryListPartitionFunction is a partition function that hashes
   a key BEFORE searching a KeyPartitionerInterface object for its partition.
   This has the benefit of reducing the size of the key to an 8 byte hash, which
   can greatlyimprove performance and robustness if the key size distribution is
   skewed.

   This partition function is NOT order-preserving, and therefore cannot be used
   when the output data needs to be sorted.
 */
class HashedBoundaryListPartitionFunction
  : public BoundaryListPartitionFunction {
public:
  /// Constructor
  /**
     \param keyPartitioner a pointer to a phase 0 key partitioner to search for
     partitions. If NULL, we don't expect to calculate partitions with this
     function, but want to query its other properties instead
   */
  HashedBoundaryListPartitionFunction(
    const KeyPartitionerInterface* keyPartitioner);

  /**
     \sa PartitionFunctionInterface::globalPartition
   */
  uint64_t globalPartition(const uint8_t* key, uint32_t keyLength) const;

  /**
     \sa PartitionFunctionInterface::localPartition
   */
  uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const;

  virtual bool hashesKeys() const;

  // Is this key accepted by the provided RecordFilter?
  bool acceptedByFilter(
    const uint8_t* key, uint32_t keyLength,
    const RecordFilter& filter) const;
};

#endif // MAPRED_HASHED_BOUNDARY_LIST_PARTITION_FUNCTION_H
