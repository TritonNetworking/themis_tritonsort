/**
    Partition Benchmark
    Author: Michael Conley. 2011.

    This benchmark exercises the possibility of a search-sort boundary based
    partitioning scheme by comparing against the existing depth-3 PartitionTrie
    solution.

    The search-sort solution uses a buffer of keys and an array of
    key-length/pointer structs that gets sorted by key. Partitions are
    calculated with a binary search on the boundaries for the GLB of a given
    key.

    The benchmark runs the following logical steps:
    1. Generate 10 million random 0-10 byte keys
      1a. Populate a PartitionTrie using these keys.
      1b. Populate the search-sort buffers with the keys.
    2. Calculate partitions.
      2a. Call trie->calculatePartitions
      2b. Sort the search-sort buffer of keys. Pick off every nth key as a
          boundary to get one boundary per partition.
    3. Compare the outputs of each on 10 sample keys for sanity.
    4. Generate 20 million random keys for each of the following:
      4a. Run the binary search on all keys and print elapsed time.
      4b. Run trie-getPartition() on all keys and print elapsed time.

    Results:
    As of May 11 2011, the boundary search-sort algorithm takes upwards of
    2.5x as much time to get partitions, but uses less memory.

    NB: This code is ugly. I am sorry.
*/

#include <iostream>
#include <queue>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include "core/Timer.h"
#include "mapreduce/common/partitioner/PartitionTrie.h"
#include "mapreduce/core/Comparison.h"

/**
   A structure representing a tuple's key
 */
typedef struct recordStruct {
  /// the length of the key
  uint32_t keyLength;
  /// a pointer to the key's data
  uint8_t* keyPtr;

  /// Constructor
  /**
     Sets the fields of the struct to the given parameters.

     \param _keyLength the length of the key
     \param _keyPtr a pointer to the key's data
   */
  recordStruct(uint32_t _keyLength, uint8_t*_keyPtr)
    : keyLength(_keyLength), keyPtr(_keyPtr) {}

  /// Default constructor
  recordStruct() {}
} record;

/// Compare two records. Conforms to qsort's comparator function interface
/**
   Uses compare to compare the two records.

   \param record1 the first record
   \param record2 the second record

   \return a negative integer if record1 < record2, a positive integer if
   record1 > record2, or 0 if the records are equal
 */
inline int compare(const void* record1, const void* record2) {
  const record* r1 = static_cast<const record*>(record1);
  const record* r2 = static_cast<const record*>(record2);

  return compare(
    r1->keyPtr, r1->keyLength, r2->keyPtr, r2->keyLength);
}

/// Do a logarithmic search through the boundary buffer for a particular record
/**
   \param targetRecord the record for which to search
   \param boundaries the boundary buffer to search
   \param lowerBound the smallest index to be considered in the buffer
   \param upperBound the largest index to be considered in the buffer

   \return the index in the boundary buffer containing the boundary closes to
   the target record
 */
uint32_t searchBoundaryBuffer(record& targetRecord,
                              record* boundaries,
                              uint32_t lowerBound,
                              uint32_t upperBound) {
  // Base case
  if (lowerBound == upperBound) {
    return lowerBound;
  }

  uint32_t midpoint = (upperBound + lowerBound) / 2;
  midpoint += (upperBound + lowerBound) % 2; // Round up if non-integer
  record& boundaryRecord = boundaries[midpoint];

  if (compare(&targetRecord, &boundaryRecord) < 0) {
    // If < midpoint, set upper = m - 1, but force upper >= 0
    upperBound = midpoint - 1 < 0 ? 0 : midpoint - 1;
  } else {
    // If >= midpoint, set lower = m
    lowerBound = midpoint;
  }

  // Recurse
  return searchBoundaryBuffer(targetRecord, boundaries, lowerBound, upperBound);
}

/// Generate a collection of records with which to simulate the performance of
/// the various partition schemes
/**
   (MC) This is a really dumb implementation.
   1) Alloc the maximum ammount of memory you will need
   2) For each record:
       a) Get a random record size between 0 and the max
       b) For each byte in the record, generate a random 8 bit number in the key

   \param numRecords the number of records to generate

   \param maxRecordSize the maximum size of a record

   \param[out] records a queue of records into which to insert the
   newly-created records

   \param[out] memLocation pointer to the allocated memory that contains random
   data for the records. It is the caller's responsiblity to free this memory
 */
void generateRecords(uint64_t numRecords, uint32_t maxRecordSize,
                     std::queue<record>& records, uint8_t*& memLocation) {

  std::cout << "Generating records..." << std::endl;

  // Alloc a bunch of memory
  memLocation = new uint8_t[numRecords * maxRecordSize];
  uint8_t* buffer = memLocation;

  for (uint64_t i = 0; i < numRecords; i++) {
    uint8_t* keyPtr = buffer;
    // Choose record size
    uint32_t recordSize = rand() % (maxRecordSize + 1);

    for (uint32_t j = 0; j < recordSize; j++) {
      uint8_t byteValue = rand() % 256;
      // Write the byte into the buffer
      *buffer = byteValue;
      // Advance the buffer pointer
      buffer++;
    }

    // Create the record
    record newRecord(recordSize, keyPtr);
    // Push it back
    records.push(newRecord);
  }

  std::cout << "Finished generating records." << std::endl;
}

int main(int argc, char** argv) {
  // Seed randomizer
  srand(time(NULL));

  uint64_t numRecords = 10000000;
  uint32_t maxRecordSize = 10;

  uint32_t numPartitions = 8 * 397 * 52;

  std::queue<record> records;
  uint8_t* memLocation = NULL;
  generateRecords(numRecords, maxRecordSize, records, memLocation);

  uint64_t depth = 3;
  uint64_t fanout = 256;

  PartitionTrie *trie = new PartitionTrie(depth, fanout);

  // Create the sort-search pre-sort buffer
  record* preSortBuffer = new record[numRecords];
  record* preSortPtr = preSortBuffer;

  std::cout << "Populating Trie and filling presort buffer..." << std::endl;

  while (!records.empty()) {
    record& newRecord = records.front();

    // Record a sample in the trie
    trie->recordSample(newRecord.keyPtr, newRecord.keyLength);

    // Copy to presort buffer
    memcpy(preSortPtr, &newRecord, sizeof(record));
    preSortPtr++;

    records.pop();
  }

  std::cout << "Finished populating trie and filling presort." << std::endl;

  std::cout << "Calculating trie partitions.." << std::endl;

  trie->calculatePartitions(numPartitions);

  std::cout << "Finished calculating trie partitions." << std::endl;

  // Sort the buffer

  std::cout << "Running qsort on presort buffer" << std::endl;
  qsort(preSortBuffer, numRecords, sizeof(record), compare);
  std::cout << "Finished qsort on presort buffer" << std::endl;

  std::cout << "Creating boundary buffer" << std::endl;

  // Pick off boundaries
  uint64_t numBPartitions = 8 * 397;
  uint8_t* boundaryKeys = new uint8_t[maxRecordSize * numBPartitions];
  uint8_t* keyBuffer = boundaryKeys;
  record* boundaryRecords = new record[numBPartitions];
  for (uint64_t p = 0; p < numBPartitions; p++) {
    // Find the correct boundary
    uint64_t boundaryStart = (p * numRecords) / numBPartitions;
    uint32_t keyLength = preSortBuffer[boundaryStart].keyLength;
    uint8_t* keyPtr = preSortBuffer[boundaryStart].keyPtr;
    // Store record info
    boundaryRecords[p].keyLength = keyLength;
    boundaryRecords[p].keyPtr = keyBuffer;
    // Copy to key to boundary keys
    memcpy(keyBuffer, keyPtr, keyLength);
    keyBuffer += keyLength;
  }

  std::cout << "Finished creating boundary buffer" << std::endl;

  // Free the presort buffer
  delete[] preSortBuffer;

  // Free memory
  delete[] memLocation;
  memLocation = NULL;

  // Generate some more data to test
  std::cout << "Generating test data" << std::endl;

  numRecords = 10;
  generateRecords(numRecords, maxRecordSize, records, memLocation);

  std::cout << "Finished generating test data" << std::endl;

  std::cout << "Calculating partitions for test data" << std::endl;

  while (!records.empty()) {
    record& newRecord = records.front();

    // Get partition from the trie
    uint32_t triePartition = trie->getPartition(newRecord.keyPtr,
                                                newRecord.keyLength);

    // Get partition from boundary buffer
    uint32_t boundaryPartition = searchBoundaryBuffer(newRecord,
                                                      boundaryRecords,
                                                      0,
                                                      numBPartitions);
    // Display
    std::ostringstream key;
    for (uint32_t i = 0; i < newRecord.keyLength; i++) {
      key << std::hex << newRecord.keyPtr[i];
    }

    std::cout << "Key [" << key.str().c_str() << "] with length ["
              << newRecord.keyLength << "] has trie partition <"
              << triePartition << "> and boundary partition <"
              << boundaryPartition << ">" << std::endl;

    records.pop();
  }

  // Free memory
  delete[] memLocation;
  memLocation = NULL;

  // Test speed
  numRecords = 20000000;
  generateRecords(numRecords, maxRecordSize, records, memLocation);
  Timer trieTimer, boundaryTimer;
  std::queue<record> recordsCopy(records);

  boundaryTimer.start();
  while (!recordsCopy.empty()) {
    record& newRecord = records.front();

    searchBoundaryBuffer(newRecord, boundaryRecords, 0, numBPartitions);

    recordsCopy.pop();
  }
  boundaryTimer.stop();
  std::cout << "boundary get elapsed = " << boundaryTimer.getElapsed() / 1000000
            << "s (" << boundaryTimer.getElapsed() << "us)" << std::endl;

  trieTimer.start();
  while (!records.empty()) {
    record& newRecord = records.front();

    trie->getPartition(newRecord.keyPtr, newRecord.keyLength);

    records.pop();
  }
  trieTimer.stop();
  std::cout << "trie get elapsed = " << trieTimer.getElapsed() / 1000000
            << "s (" << trieTimer.getElapsed() << "us)" << std::endl;


  delete[] memLocation;

  delete trie;

  delete[] boundaryRecords;
  delete[] boundaryKeys;

  return 0;
}
