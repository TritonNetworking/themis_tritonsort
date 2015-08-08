#include "common/SimpleMemoryAllocator.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/HeapEntryPtrComparator.h"
#include "mapreduce/common/boundary/KeyPartitioner.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "tests/mapreduce/common/BoundaryListTest.h"

extern const char* TEST_WRITE_ROOT;

void BoundaryListTest::testTupleHeap() {
  // Make sure the TupleHeap data structure used in BoundaryCalculator actually
  // sorts tuples correctly.
  // This is more of a test of our heap entry comparator function than anything,
  // since the heap is implemented as an stl priority queue

  // Note the TupleHeap is only supposed to sort on keys!
  TupleHeap heap;

  // Create tuples:
  // Tuple 1 is 2/8 bytes with key = 5 5
  KeyValuePair tuple1;
  uint8_t* buffer1 = generateRepeatingTuple(tuple1, 2, 8, 5);
  // Tuple 2 is 2/10 bytes with key = 0 0
  KeyValuePair tuple2;
  uint8_t* buffer2 = generateRepeatingTuple(tuple2, 2, 10, 0);
  // Tuple 3 is 1/5 bytes with key = 80
  KeyValuePair tuple3;
  uint8_t* buffer3 = generateRepeatingTuple(tuple3, 1, 5, 80);
  // Tuple 4 is 4/9 bytes with key = 10 10 10 10
  KeyValuePair tuple4;
  uint8_t* buffer4 = generateRepeatingTuple(tuple4, 4, 9, 10);
  // Tuple 5 is 3/9 bytes with key = 10 10 10
  KeyValuePair tuple5;
  uint8_t* buffer5 = generateRepeatingTuple(tuple5, 3, 9, 10);
  // Tuple 6 is 100/10 bytes with key = 3 3 3 ...
  KeyValuePair tuple6;
  uint8_t* buffer6 = generateRepeatingTuple(tuple6, 100, 10, 3);

  // Make heap entries associated with each tuple
  HeapEntry h1(tuple1);
  h1.id = 1;
  HeapEntry h2(tuple2);
  h2.id = 2;
  HeapEntry h3(tuple3);
  h3.id = 3;
  HeapEntry h4(tuple4);
  h4.id = 4;
  HeapEntry h5(tuple5);
  h5.id = 5;
  HeapEntry h6(tuple6);
  h6.id = 6;

  // Push tuples onto the heap
  heap.push(&h1);
  heap.push(&h2);
  heap.push(&h3);
  heap.push(&h4);
  heap.push(&h5);
  heap.push(&h6);

  // Fetch tuples from heap
  uint64_t expectedIDs[6] = {2, 6, 1, 5, 4, 3};

  for (unsigned int i = 0; i < 6; i ++) {
    CPPUNIT_ASSERT_MESSAGE("Heap pre-maturely empty", !heap.empty());
    HeapEntry* top = heap.top();
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Heap out of order", expectedIDs[i], top->id);
    heap.pop();
  }
  CPPUNIT_ASSERT_MESSAGE("Heap not empty all tuples popped", heap.empty());

  // Cleanup
  delete[] buffer1;
  delete[] buffer2;
  delete[] buffer3;
  delete[] buffer4;
  delete[] buffer5;
  delete[] buffer6;
}

void BoundaryListTest::testLocalPartitionsInternal(
  KeyPartitionerInterface* keyPartitionerPtr) {

  KeyValuePair tuple;
  KeyPartitionerInterface& keyPartitioner = *keyPartitionerPtr;

  // Local partitions for node 1 are 4,5,6,7
  // Also pass disk 0 for partitions 4 and 5, and disk 1 for partitions 6 and 7
  // Verify key 100 100 100 belongs to partition 4
  verifyLocalPartitionFunction(keyPartitioner, tuple, 3, 0, 100, 4, 0);

  // Verify key 101 belongs to partition 4     - because 101 < 101 101
  verifyLocalPartitionFunction(keyPartitioner, tuple, 1, 0, 101, 4, 0);

  // Verify key 101 101 belongs to partition 5  - because equality rounds up to
  // the next partition
  verifyLocalPartitionFunction(keyPartitioner, tuple, 2, 0, 101, 5, 0);

  // Verify key 119 119 belongs to partition 5
  verifyLocalPartitionFunction(keyPartitioner, tuple, 2, 0, 119, 5, 0);

  // Verify key 120 belongs to partition 5
  verifyLocalPartitionFunction(keyPartitioner, tuple, 1, 0, 120, 5, 0);

  // Verify key 120 120 120 120 120 belongs to partition 6
  verifyLocalPartitionFunction(keyPartitioner, tuple, 5, 0, 120, 6, 1);

  // Verify key 130 belongs to partition 7
  verifyLocalPartitionFunction(keyPartitioner, tuple, 1, 0, 130, 7, 1);

  // Verify (key 179 179 ..) 90 times belongs to partition 7
  verifyLocalPartitionFunction(keyPartitioner, tuple, 90, 0, 179, 7, 1);
}

void BoundaryListTest::testLocalPartitions() {
  uint64_t numNodes = 3;
  uint64_t numPhysicalDisksPerNode = 2;
  uint64_t numLogicalDisksPerPhysicalDisk = 2;
  // Use 6 partition groups so that each disk gets its own global partition
  uint64_t numPartitionGroups = 6;
  uint64_t nodeID = 1;
  KeyValuePair tuple;

  KeyPartitionerInterface* keyPartitionerPtr = createNewKeyPartitioner(
    numNodes, numPhysicalDisksPerNode, numLogicalDisksPerPhysicalDisk, nodeID,
    numPartitionGroups);

  testLocalPartitionsInternal(keyPartitionerPtr);

  // Verify that 0th partition accepts smaller tuples. The 0th partition isn't a
  // true lower bound.
  nodeID = 0;

  KeyPartitionerInterface* keyPartitionerPtr2 = createNewKeyPartitioner(
    numNodes, numPhysicalDisksPerNode, numLogicalDisksPerPhysicalDisk, nodeID,
    numPartitionGroups);

  // Sanity check: keyPartitioner and keyPartitioner2 should not be equal
  CPPUNIT_ASSERT(!keyPartitionerPtr->equals(*keyPartitionerPtr2));

  KeyPartitionerInterface& keyPartitioner2 = *keyPartitionerPtr2;

  // Verify key 0 0 belongs to partition 0     - 0 is a special case, tuples
  // below 0 hash to 0
  verifyLocalPartitionFunction(keyPartitioner2, tuple, 2, 0, 0, 0, 0);

  // Verify key 1 1 1 belongs to partition 0   - equal to counts as lower bound
  verifyLocalPartitionFunction(keyPartitioner2, tuple, 3, 0, 1, 0, 0);

  // Verify key 10 10 belongs to partition 1
  verifyLocalPartitionFunction(keyPartitioner2, tuple, 2, 0, 10, 1, 0);

  delete keyPartitionerPtr2;

  delete keyPartitionerPtr;
}

void BoundaryListTest::testGlobalPartitions() {
  // Create the partition boundaries
  KeyValuePair tuple;
  uint8_t* byteBuffer;
  // Partition 0 = 1 1 1
  // Partition 1 = 10 10
  // Partition 2 = 35
  // Partition 3 = 50 50 50 50 50
  // Partition 4 = 100 100
  // Partition 5 = 101 101
  // Partition 6 = 120 120 120 120
  // Partition 7 = 130
  // Partition 8 = 180 180
  // Partition 9 = 200
  // Partition 10 = 220 220 ... - 80 bytes worth
  // Partition 11 = 255
  uint32_t keyLengths[12] =    {3,  2,  1,  5,   2,   2,   4,   1,   2,   1,
                                80,   1};
  unsigned char keyBytes[12] = {1, 10, 35, 50, 100, 101, 120, 130, 180, 200,
                                220, 255};

  // Create tuples and copy to buffer - 4K to be safe and no alignment
  KVPairBuffer kvPairBuffer(*memoryAllocator, callerID, 4096, 0);
  for (unsigned int i = 0; i < 12; i++) {
    // Create tuple
    byteBuffer = generateRepeatingTuple(tuple, keyLengths[i], 0,
                                        keyBytes[i]);
    // Copy to buffer
    kvPairBuffer.addKVPair(tuple);

    // Free the byte buffer so we don't leak memory
    delete[] byteBuffer;
  }
  kvPairBuffer.resetIterator();

  uint64_t numNodes = 3;
  uint64_t numPhysicalDisksPerNode = 2;
  uint64_t numLogicalDisksPerPhysicalDisk = 2;
  // Set partition groups to 2 to make sure we get 1 partition group per disk
  uint64_t numPartitionGroups = 6;
  uint64_t nodeID = 1;

  uint64_t numPartitions =
    numLogicalDisksPerPhysicalDisk * numPhysicalDisksPerNode * numNodes;

  // Use 12 partitions, 6 global 2 local per global
  // Use node 1 which is the middle node
  KeyPartitioner keyPartitioner(
    kvPairBuffer, nodeID, numNodes, numPartitionGroups, numPartitions);
  kvPairBuffer.resetIterator();

  // Verify key 0 0 belongs to partition 0
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 2, 0, 0, 0);

  // Verify key 40 belongs to partition 1
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 1, 0, 40, 1);

  // Verify key 100 belongs to partition 1
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 1, 0, 100, 1);

  // Verify key 100 100 belongs to partition 2
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 2, 0, 100, 2);

  // Verify key 125 x 8 belongs to partition 3
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 8, 0, 125, 3);

  // Verify key 243 243 243 belongs to partition 5
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 3, 0, 243, 5);

  // Verify key 254 belongs to partition 5
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 1, 0, 254, 5);

  // Verify key 255 belongs to partition 5
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 1, 0, 255, 5);

  // Verify key 255 255 255 255 belongs to partition 5
  verifyGlobalPartitionFunction(keyPartitioner, tuple, 4, 0, 255, 5);
}

// =====
// Helper functions
// =====

uint8_t* BoundaryListTest::generateRepeatingTuple(
  KeyValuePair& tuple, uint32_t keyLength, uint32_t valueLength,
  unsigned char keyByteValue) {
  // Create a byte array
  uint8_t* buffer = new uint8_t[
    KeyValuePair::tupleSize(keyLength, valueLength)];

  // Fill the key
  memset(KeyValuePair::key(buffer), keyByteValue, keyLength);

  // Set the header
  tuple.setKey(NULL, keyLength);
  tuple.setValue(NULL, valueLength);
  tuple.serializeHeader(buffer);

  // Grab the appropriate key and value pointers
  tuple.deserialize(buffer);

  // Let the test delete the buffer
  return buffer;
}

void BoundaryListTest::verifyLocalPartitionFunction(
  KeyPartitionerInterface& keyPartitioner, KeyValuePair& tuple,
  uint32_t keyLength, uint32_t valueLength, unsigned char keyByteValue,
  uint64_t expectedPartition, uint64_t partitionGroup) {

  uint8_t* byteBuffer = generateRepeatingTuple(
    tuple, keyLength, valueLength, keyByteValue);
  uint64_t partition = keyPartitioner.localPartition(
    tuple.getKey(), tuple.getKeyLength(), partitionGroup);
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Bad local partition function",
                               expectedPartition, partition);
  delete[] byteBuffer;
}

void BoundaryListTest::verifyGlobalPartitionFunction(
  KeyPartitionerInterface& keyPartitioner, KeyValuePair& tuple,
  uint32_t keyLength, uint32_t valueLength, unsigned char keyByteValue,
  uint64_t expectedPartition) {

  uint8_t* byteBuffer = generateRepeatingTuple(
    tuple, keyLength, valueLength, keyByteValue);
  uint64_t partition = keyPartitioner.globalPartition(
    tuple.getKey(), tuple.getKeyLength());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Bad global partition function",
                               expectedPartition, partition);
  delete[] byteBuffer;
}

KeyPartitionerInterface* BoundaryListTest::createNewKeyPartitioner(
  uint64_t numNodes, uint64_t numPhysicalDisksPerNode,
  uint64_t numLogicalDisksPerPhysicalDisk, uint64_t nodeID,
  uint64_t numPartitionGroups) {

  // Create the partition boundaries
  KeyValuePair tuple;
  uint8_t* byteBuffer;
  // Partition 0 = 1 1 1
  // Partition 1 = 10 10
  // Partition 2 = 35
  // Partition 3 = 50 50 50 50 50
  // Partition 4 = 100 100
  // Partition 5 = 101 101
  // Partition 6 = 120 120 120 120
  // Partition 7 = 130
  // Partition 8 = 180 180
  // Partition 9 = 200
  // Partition 10 = 220 220 ... - 80 bytes worth
  // Partition 11 = 255
  uint32_t keyLengths[12] =    {3,  2,  1,  5,   2,   2,   4,   1,   2,   1,
                                80,   1};
  unsigned char keyBytes[12] = {1, 10, 35, 50, 100, 101, 120, 130, 180, 200,
                                220, 255};

  // Create tuples and copy to buffer - 4K to be safe and no alignment
  KVPairBuffer kvPairBuffer(*memoryAllocator, callerID, 4096, 0);
  for (unsigned int i = 0; i < 12; i++) {
    // Create tuple
    byteBuffer = generateRepeatingTuple(tuple, keyLengths[i], 0,
                                        keyBytes[i]);
    // Copy to buffer
    kvPairBuffer.addKVPair(tuple);
    // Free the byte buffer so we don't leak memory
    delete[] byteBuffer;
  }

  kvPairBuffer.resetIterator();

  uint64_t numPartitions =
    numLogicalDisksPerPhysicalDisk * numPhysicalDisksPerNode * numNodes;

  // Create a KeyPartitioner object
  KeyPartitionerInterface* keyPartitioner = new KeyPartitioner(
    kvPairBuffer, nodeID, numNodes, numPartitionGroups, numPartitions);
  kvPairBuffer.resetIterator();

  return keyPartitioner;
}

void BoundaryListTest::testWriteToFileAndLoad() {
  uint64_t numNodes = 3;
  uint64_t numPhysicalDisksPerNode = 2;
  uint64_t numLogicalDisksPerPhysicalDisk = 2;
  uint64_t numPartitionGroups = 6;
  uint64_t nodeID = 1;

  KeyPartitionerInterface* keyPartitioner = createNewKeyPartitioner(
    numNodes, numPhysicalDisksPerNode, numLogicalDisksPerPhysicalDisk, nodeID,
    numPartitionGroups);

  // Sanity check: boundary list should equal itself
  CPPUNIT_ASSERT(keyPartitioner->equals(*keyPartitioner));

  // Dump boundary list to file, then read it back again

  std::ostringstream oss;
  oss << TEST_WRITE_ROOT << "/boundary_list_dump";

  File file(oss.str());

  keyPartitioner->writeToFile(file);

  KeyPartitioner loadedKeyPartitioner(file);

  CPPUNIT_ASSERT(keyPartitioner->equals(loadedKeyPartitioner));
  CPPUNIT_ASSERT(loadedKeyPartitioner.equals(*keyPartitioner));

  file.unlink();

  delete keyPartitioner;
}
