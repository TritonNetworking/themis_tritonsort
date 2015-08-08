#ifndef MAPRED_BOUNDARY_LIST_PARTITION_FUNCTION_H
#define MAPRED_BOUNDARY_LIST_PARTITION_FUNCTION_H

#include "mapreduce/common/PartitionFunctionInterface.h"

class KeyPartitionerInterface;

/**
   BoundaryListPartitionFunction is an order-preserving partition function that
   partitions keys using a KeyPartitionerInterface object created in phase 0.
   KeyPartitioners contain searchable lists of boundary keys that are used to
   determine global and local partitions.
 */
class BoundaryListPartitionFunction : public PartitionFunctionInterface {
public:
  /// Constructor
  /**
     \param keyPartitioner a pointer to a phase 0 key partition to search for
     partitions. If NULL, we don't expect to use this partition function for
     calculating partitions, but need to query its other properties
   */
  BoundaryListPartitionFunction(const KeyPartitionerInterface* keyPartitioner);

  /**
     \sa PartitionFunctionInterface::globalPartition
   */
  uint64_t globalPartition(const uint8_t* key, uint32_t keyLength) const;

  /**
     \sa PartitionFunctionInterface::localPartition
   */
  uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const;

  bool hashesKeys() const;

  bool acceptedByFilter(
    const uint8_t* key, uint32_t keyLength,
    const RecordFilter& filter) const;

  uint64_t numGlobalPartitions() const;

private:
  const KeyPartitionerInterface* keyPartitioner;
};

#endif // MAPRED_BOUNDARY_LIST_PARTITION_FUNCTION_H
