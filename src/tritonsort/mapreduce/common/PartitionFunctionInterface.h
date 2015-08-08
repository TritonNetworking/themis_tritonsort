#ifndef MAPRED_PARTITION_FUNCTION_INTERFACE_H
#define MAPRED_PARTITION_FUNCTION_INTERFACE_H

#include <stdint.h>

#include "mapreduce/common/filter/RecordFilter.h"

/**
   A PartitionFunctionInterface partitions the key space for tuple routing
   purposes. Keys can be partitioned globally to determine to which node a key
   belongs. Keys can be further partitioned locally to determine to which
   logical disk a key belongs.

   In general, the global partitioning function will be identical for all nodes,
   while the local partitioning function can be unique to a single node.
 */
class PartitionFunctionInterface {
public:
  /// Destructor
  virtual ~PartitionFunctionInterface() {}

  /**
     Compute a key's global partition for node routing purposes.

     \param key the key to be partitioned

     \param keyLength the length of the key

     \return the global partition identifier for the key
   */
  virtual uint64_t globalPartition(
    const uint8_t* key, uint32_t keyLength) const = 0;

  /**
     Compute a key's local partition for logical disk routing purposes given
     that you know which physical disk the key belongs to.

     \param key the key to be partitioned

     \param keyLength the length of the key

     \param partitionGroup the partition group corresponding to the key

     \return the local partition identifier for the key
   */
  virtual uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const = 0;

  /// Does this partition function hash keys?
  /**
     \return true if keys are hashed, and false otherwise
   */
  virtual bool hashesKeys() const = 0;

  // Is this key accepted by the provided RecordFilter?
  virtual bool acceptedByFilter(
    const uint8_t* key, uint32_t keyLength,
    const RecordFilter& filter) const = 0;

  /// \return the number of global partitions created by this partition function
  virtual uint64_t numGlobalPartitions() const = 0;
};

#endif // MAPRED_PARTITION_FUNCTION_INTERFACE_H
