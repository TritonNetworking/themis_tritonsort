#ifndef THEMIS_UNIFORM_PARTITION_FUNCTION_H
#define THEMIS_UNIFORM_PARTITION_FUNCTION_H

#include "core/constants.h"
#include "mapreduce/common/PartitionFunctionInterface.h"

class Params;

/**
   UniformPartitionFunction is a partition function that partitions keys
   uniformly across the key space using the first 3 bytes of the key.

   \warning All keys must be at least 3 bytes long for this partition function
   to produce correct results.
 */
class UniformPartitionFunction
  : public PartitionFunctionInterface {
public:
  UniformPartitionFunction(
    const Params& params, uint64_t numPartitions);

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
  /**
     Compute a raw hash on the first three bytes of the key by interpreting
     the bytes in binary. Special cases for keys that are 0, 1, and 2 bytes long
     ensure that these short keys get placed in separate divisions from longer
     keys. The value returned by this hash function varies between 0 and
     MAX_PARTITIONS, and must be scaled down by the appropriate partitioning
     method.

     \param key the key to hash

     \param keyLength the length of the key

     \return a raw hash value on the key that varies from 0 to MAX_PARTITIONS
  */
  inline uint64_t hash(const uint8_t* key, uint32_t keyLength) const {
    ASSERT(keyLength >= 3, "UniformPartitionFunction requires keys to be at "
           "least 3 bytes long. Got %llu", keyLength);
    uint64_t hashedKey = 0;
    for (uint32_t i = 0; i < 3; i++) {
      hashedKey += key[i] << (2 - i) * 8;
    }

    return hashedKey;
  }

  const static uint64_t MAX_PARTITIONS = 16777216;

  const uint64_t globalPartitions;
  const uint64_t localPartitions;
};


#endif // THEMIS_UNIFORM_PARTITION_FUNCTION_H
