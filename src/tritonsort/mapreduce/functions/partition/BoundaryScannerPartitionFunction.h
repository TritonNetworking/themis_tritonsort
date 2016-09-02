
#ifndef THEMIS_BOUNDARY_SCANNER_PARTITION_FUNCTION_H
#define THEMIS_BOUNDARY_SCANNER_PARTITION_FUNCTION_H

#include "tritonsort/core/constants.h"
#include "tritonsort/core/TritonSortAssert.h"
#include "tritonsort/mapreduce/common/PartitionFunctionInterface.h"

/**
   BoundaryScannerPartitionFunction is a partition function assigns tuples to
   specific nodes depending on how many boundary keys have been picked by the
   BoundaryScanner. The function maintains a count of the the number of keys
   and the current node and increments the node count every time the number of
   keys passes the number of partitions per node.
 */
class BoundaryScannerPartitionFunction
  : public PartitionFunctionInterface {
public:
  BoundaryScannerPartitionFunction(
    uint64_t partitionsPerNode, uint64_t numNodes);

  uint64_t globalPartition(const uint8_t* key, uint32_t keyLength) const;

  uint64_t localPartition(const uint8_t* key, uint32_t keyLength) const;

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
  const uint64_t partitionsPerNode;
  const uint64_t numNodes;

  mutable uint64_t partitionsSeen;
};

#endif // THEMIS_BOUNDARY_SCANNER_PARTITION_FUNCTION_H
