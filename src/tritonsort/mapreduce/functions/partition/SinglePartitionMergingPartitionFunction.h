#ifndef THEMIS_SINGLE_PARTITION_MERGING_PARTITION_FUNCTION_H
#define THEMIS_SINGLE_PARTITION_MERGING_PARTITION_FUNCTION_H

#include "core/constants.h"
#include "mapreduce/common/PartitionFunctionInterface.h"

class SinglePartitionMergingPartitionFunction
  : public PartitionFunctionInterface {
public:
  SinglePartitionMergingPartitionFunction();

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
  DISALLOW_COPY_AND_ASSIGN(SinglePartitionMergingPartitionFunction);
};


#endif // THEMIS_SINGLE_PARTITION_MERGING_PARTITION_FUNCTION_H
