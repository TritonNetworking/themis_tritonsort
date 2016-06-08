#ifndef MAPRED_BOUNDARY_LIST_TEST_H
#define MAPRED_BOUNDARY_LIST_TEST_H

#include "tritonsort/config.h"
#include "core/MemoryAllocatorInterface.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/common/MemoryAllocatingTestFixture.h"

class KeyPartitionerInterface;

class BoundaryListTest : public MemoryAllocatingTestFixture {
protected:
  KeyPartitionerInterface* createNewKeyPartitioner(
    uint64_t numNodes, uint64_t numPhysicalDisksPerNode,
    uint64_t numLogicalDisksPerPhysicalDisk, uint64_t nodeID,
    uint64_t numPartitionGroups);

  void testLocalPartitionsInternal(
    KeyPartitionerInterface* keyPartitionerPtr);

  uint8_t* generateRepeatingTuple(KeyValuePair& tuple,
                                  uint32_t keyLength, uint32_t valueLength,
                                  unsigned char keyByteValue);

  void verifyLocalPartitionFunction(
    KeyPartitionerInterface& keyPartitioner, KeyValuePair& tuple,
    uint32_t keyLength, uint32_t valueLength, unsigned char keyByteValue,
    uint64_t expectedPartition, uint64_t partitionGroup);

  void verifyGlobalPartitionFunction(
    KeyPartitionerInterface& keyPartitioner, KeyValuePair& tuple,
    uint32_t keyLength, uint32_t valueLength, unsigned char keyByteValue,
    uint64_t expectedPartition);
};

#endif //MAPRED_BOUNDARY_LIST_TEST_H
