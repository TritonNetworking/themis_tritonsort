#include "core/ByteOrder.h"
#include "core/File.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/boundary/KeyList.h"
#include "mapreduce/common/boundary/KeyPartitioner.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

KeyPartitioner::KeyPartitioner(
  KVPairBuffer& partitionBoundaryBuffer, uint64_t localNodeID,
  uint64_t numNodes, uint64_t numPartitionGroups, uint64_t numPartitions) {
  uint64_t partitionsPerGroup = numPartitions / numPartitionGroups;
  uint64_t partitionsPerNode = numPartitions / numNodes;

  // Iterate over the boundary buffer to determine how large to make key lists
  uint64_t startPosition = partitionBoundaryBuffer.getIteratorPosition();
  uint64_t globalBytes = 0;
  uint64_t localBytes = 0;
  uint64_t partition = 0;
  KeyValuePair kvPair;
  while (partitionBoundaryBuffer.getNextKVPair(kvPair)) {
    if (partition % partitionsPerGroup == 0) {
      // This is the first partition of a new partition group.
      globalBytes += kvPair.getKeyLength();
    }

    if (partition / partitionsPerNode == localNodeID) {
      // This partition belongs to this node.
      localBytes += kvPair.getKeyLength();

      if (partition % partitionsPerGroup == partitionsPerGroup - 1) {
        // This the last partition for this partition group.
        uint64_t firstPartition = partition - (partitionsPerGroup - 1);
        localKeyLists.push_back(
          new (themis::memcheck) KeyList(
            partitionsPerGroup, localBytes, firstPartition));

        // Reset the local byte count for the next partition group.
        localBytes = 0;
      }
    }

    partition++;
  }

  // Verify we have the correct number of partitions in the buffer.
  ASSERT(partition == numPartitions,
         "Partition Boundary buffer has %llu partitions, but we are expecting "
         "%llu partitions", partition, numPartitions);

  // Create the global key list.
  globalKeyList =
    new (themis::memcheck) KeyList(numPartitionGroups, globalBytes);

  // Iterate over the buffer again, this time assigning keys to key lists.
  partitionBoundaryBuffer.setIteratorPosition(startPosition);
  partition = 0;
  KeyListVector::iterator localKeyListIter = localKeyLists.begin();
  while (partitionBoundaryBuffer.getNextKVPair(kvPair)) {
    if (partition % partitionsPerGroup == 0) {
      // This is the first partition of a new partition group.
      globalKeyList->addKey(kvPair.getKey(), kvPair.getKeyLength());
    }

    if (partition / partitionsPerNode == localNodeID) {
      // This partition belongs to this node.
      (*localKeyListIter)->addKey(kvPair.getKey(), kvPair.getKeyLength());

      if (partition % partitionsPerGroup == partitionsPerGroup - 1) {
        // This the last partition for this partition group.
        localKeyListIter++;
      }
    }

    partition++;
  }
}

KeyPartitioner::KeyPartitioner(File& file) {
  file.open(File::READ);

  // The first few bytes of the file will tell us the number of key lists to
  // create.
  uint64_t numLocalKeyLists;
  file.read(reinterpret_cast<uint8_t*>(
              &numLocalKeyLists), sizeof(numLocalKeyLists));
  numLocalKeyLists = bigEndianToHost64(numLocalKeyLists);
  localKeyLists.resize(numLocalKeyLists);

  // Assume that the rest of the file is the concatentation of the key lists
  // and load them one at a time.
  globalKeyList = KeyList::newKeyListFromFile(file);
  for (KeyListVector::iterator iter = localKeyLists.begin();
       iter != localKeyLists.end(); iter++) {
    *iter = KeyList::newKeyListFromFile(file);
  }

  file.close();
}

KeyPartitioner::~KeyPartitioner() {
  if (globalKeyList != NULL) {
    delete globalKeyList;
    globalKeyList = NULL;
  }

  for (KeyListVector::iterator iter = localKeyLists.begin();
       iter != localKeyLists.end(); iter++) {
    if (*iter != NULL) {
      delete *iter;
      *iter = NULL;
    }
  }
}

uint64_t KeyPartitioner::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  return globalKeyList->findLowerBound(key, keyLength);
}

uint64_t KeyPartitioner::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  ASSERT(partitionGroup < localKeyLists.size(),
         "Tried to compute local partition for group %llu, but only %llu "
         "groups (demuxes) per node.", partitionGroup, localKeyLists.size());

  return localKeyLists[partitionGroup]->findLowerBound(key, keyLength);
}

uint64_t KeyPartitioner::getCurrentSize() const {
  uint64_t totalKeyListSize = globalKeyList->getCurrentSize();
  for (KeyListVector::const_iterator iter = localKeyLists.begin();
       iter != localKeyLists.end(); iter++) {
    totalKeyListSize += (*iter)->getCurrentSize();
  }

  return totalKeyListSize;
}

void KeyPartitioner::writeToFile(File& file) const {
  file.open(File::WRITE, true);

  // Write the number of local key lists first so we know how many KeyLists to
  // create when deserializing from the file.
  uint64_t numLocalKeyLists = hostToBigEndian64(localKeyLists.size());
  file.write(reinterpret_cast<uint8_t*>(
               &numLocalKeyLists), sizeof(numLocalKeyLists));

  // Concatenate the key lists and write them to the file.
  globalKeyList->writeToFile(file);

  for (KeyListVector::const_iterator iter = localKeyLists.begin();
       iter != localKeyLists.end(); iter++) {
    (*iter)->writeToFile(file);
  }

  file.close();
}

bool KeyPartitioner::equals(const KeyPartitionerInterface& other) const {
  const KeyPartitioner* otherPtr = dynamic_cast<const KeyPartitioner*>(&other);
  if (otherPtr == NULL) {
    // Not a KeyPartitioner object.
    return false;
  }

  // Check if global boundary keys are equal.
  if (!globalKeyList->equals(*(otherPtr->globalKeyList))) {
    return false;
  }

  // Check if local boundary keys are equal.
  if (localKeyLists.size() != otherPtr->localKeyLists.size()) {
    return false;
  }

  for (KeyListVector::const_iterator iter = localKeyLists.begin(),
         iter2 = otherPtr->localKeyLists.begin();
       iter != localKeyLists.end(); iter++, iter2++) {
    if (!(*iter)->equals(**iter2)) {
      return false;
    }
  }

  return true;
}

uint64_t KeyPartitioner::numGlobalPartitions() const {
  return globalKeyList->getNumKeys();
}
