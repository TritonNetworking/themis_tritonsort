#ifndef MAPRED_BOUNDARY_LIST_TEST_H
#define MAPRED_BOUNDARY_LIST_TEST_H

#include "config.h"
#include "core/MemoryAllocatorInterface.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/common/MemoryAllocatingTestFixture.h"

class KeyPartitionerInterface;

class BoundaryListTest : public MemoryAllocatingTestFixture {
  CPPUNIT_TEST_SUITE( BoundaryListTest );
#ifdef TRITONSORT_ASSERTS
  CPPUNIT_TEST( testLocalPartitions );
#endif //TRITONSORT_ASSERTS

  CPPUNIT_TEST( testTupleHeap );
  CPPUNIT_TEST( testGlobalPartitions );
  CPPUNIT_TEST( testWriteToFileAndLoad );
  CPPUNIT_TEST_SUITE_END();
public:
  void testTupleHeap();
  void testLocalPartitions();
  void testGlobalPartitions();
  void testWriteToFileAndLoad();

private:
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
