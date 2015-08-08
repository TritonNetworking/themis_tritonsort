#ifndef KEY_PARTITIONER_H
#define KEY_PARTITIONER_H

#include "core/constants.h"
#include "mapreduce/common/boundary/KeyPartitionerInterface.h"

class KVPairBuffer;
class KeyList;

/**
   KeyPartitioner divides partitions into groups. Keys are globally assigned
   to their partition groups, and locally assigned to their individual
   partition within the group. The goal is to divide work roughly evenly
   among the demuxes in phase one while still supporting an arbitrary number of
   demuxes.
 */
class KeyPartitioner : public KeyPartitionerInterface {
public:
  /// Constructor
  /**
     \param partitionBoundaryBuffer a KVPairBuffer containing boundary keys for
     all partitions in the cluster

     \param localNodeID the ID of this node

     \param numNodes the number of nodes in the cluster

     \param numPartitionGroups the number of partition groups in the cluster

     \param numPartitions the number of partitions created by the job associated
     with this KeyPartitioner
   */
  KeyPartitioner(
    KVPairBuffer& partitionBoundaryBuffer, uint64_t localNodeID,
    uint64_t numNodes, uint64_t numPartitionGroups, uint64_t numPartitions);

  /// Constructor
  /**
     Construct a KeyPartitioner based on the on-disk representation written
     by writeToFile()

     \param file a file containing the serialized representation of a
     KeyPartitioner
   */
  KeyPartitioner(File& file);

  /// Destructor
  virtual ~KeyPartitioner();

  /**
     Assign a key globally to its partition group.

     \param key the key to partition

     \param keyLength the length of the key

     \return the partition group for the key
   */
  uint64_t globalPartition(const uint8_t* key, uint32_t keyLength) const;

  /**
     Assign a key locally to its partition.

     \param key the key to partition

     \param keyLength the length of the key

     \param partitionGroup the global partition group for this key

     \return the parittion this key is assigned to
   */
  uint64_t localPartition(
    const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const;

  /// \sa Resource::getCurrentSize()
  uint64_t getCurrentSize() const;

  /**
     Write the KeyPartitioner to a file by concatenating its global and
     local KeyList objects.

     \param file the file to write to
   */
  void writeToFile(File& file) const;

  /**
     Check if this KeyPartitioner has the same global and local KeyList
     objects as another.

     \param other the other KeyPartitioner to check for equality

     \return true if both KeyPartitioners has the same global and local
     KeyLists
   */
  bool equals(const KeyPartitionerInterface& other) const;

  /// \return the number of partition groups in the cluster
  uint64_t numGlobalPartitions() const;

private:
  DISALLOW_COPY_AND_ASSIGN(KeyPartitioner);

  typedef std::vector<KeyList*> KeyListVector;

  KeyList* globalKeyList;
  KeyListVector localKeyLists;
};

#endif // KEY_PARTITIONER_H
