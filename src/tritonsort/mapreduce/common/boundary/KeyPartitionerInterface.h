#ifndef MAPRED_KEY_PARTITIONER_INTERFACE_H
#define MAPRED_KEY_PARTITIONER_INTERFACE_H

#include "core/Resource.h"
#include "core/TritonSortAssert.h"

class File;

/**
   A KeyPartitioner is a wrapper object around some number of KeyLists that
   exposes two partitioning functions: globalPartition() and localPartition().
   KeyPartitioners are used by PartitionFunctions to assign partitions to keys
   in the Mapper (global) and Demux (local) stages.{
 */
class KeyPartitionerInterface : public Resource {
public:
  /// Destructor
  virtual ~KeyPartitionerInterface() {}

  /**
     Compute the global partition corresponding to a key.

     \param key the key to partition

     \param keyLength the length of the key

     \return the global partition for the key
   */
  virtual uint64_t globalPartition(const uint8_t* key, uint32_t keyLength)
    const=0;

  /**
     Compute the local partition corresponding to a key.

     \param key the key to partition

     \param keyLength the length of the key

     \param hint a hint about what the partition might be; implementation
     specific

     \return the local partition for the key
   */
  virtual uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t hint) const=0;

  /// \sa Resource::getCurrentSize()
  virtual uint64_t getCurrentSize() const=0;

  /**
     Write the KeyPartitioner to a file so that it can be loaded at a later
     time.

     \param file the file to write to
   */
  virtual void writeToFile(File& file) const=0;

  /**
     Check if this KeyPartitioner assigns partitions in the same way as some
     other KeyPartitioner.

     \param other the other KeyPartitioner to check for equality

     \return true if both KeyPartitioners assign the same partitions
   */
  virtual bool equals(const KeyPartitionerInterface& other) const=0;

  /// \return the number of global partitions created by this partititioner
  virtual uint64_t numGlobalPartitions() const=0;
};

#endif // MAPRED_KEY_PARTITIONER_INTERFACE_H
