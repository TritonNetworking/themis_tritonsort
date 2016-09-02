#ifndef THEMIS_BUFFER_TABLE_H
#define THEMIS_BUFFER_TABLE_H

#include <set>

#include "common/buffers/BufferList.h"
#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"

/**
   BufferTable is a set of BufferLists, one per logical disk.

   \TODO(MC): Refactor this data structure in light of the removal of
   LogicalDiskCounts
 */
template <typename T> class BufferTable {
public:
  /// Constructor
  /**
     \param _basePhysicalDiskID the lowest-numbered physical disk ID for lists
     in this table

     \param _numPhysicalDisks the number of physical disks this table will
     support

     \param nodeID the node ID of the node that contains this BufferTable

     \param _partitionsPerDisk the number of partitions on each disk

     \param _numIntermediateDisks the number of intermediate disks per node
   */
  BufferTable(uint64_t _basePhysicalDiskID,
              uint64_t _numPhysicalDisks,
              uint64_t nodeID,
              uint64_t _partitionsPerDisk,
              uint64_t _numIntermediateDisks)
    : basePhysicalDiskID(_basePhysicalDiskID),
      numPhysicalDisks(_numPhysicalDisks),
      partitionsPerDisk(_partitionsPerDisk),
      numIntermediateDisks(_numIntermediateDisks) {

    lists = new BufferList<T>**[numPhysicalDisks];
    numLogicalDisksForPhysicalDisk = new uint64_t[numPhysicalDisks];
    largestLists = new BufferList<T>*[numPhysicalDisks];

    // Start with the first pertation for the first disk for this table
    uint64_t currentPartition =
      (nodeID * numIntermediateDisks + basePhysicalDiskID) * partitionsPerDisk;

    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      numLogicalDisksForPhysicalDisk[i] = partitionsPerDisk;

      lists[i] = new BufferList<T>*[partitionsPerDisk];

      for (uint64_t j = 0; j < partitionsPerDisk; j++) {
        lists[i][j] = new BufferList<T>(
          currentPartition, basePhysicalDiskID + i);
        currentPartition++;
      }

      largestLists[i] = NULL;
    }
  }

  /// Destructor
  virtual ~BufferTable() {
    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      uint64_t currentNumLogicalDisks = numLogicalDisksForPhysicalDisk[i];

      for (uint64_t j = 0; j < currentNumLogicalDisks; j++) {
        BufferList<T>* list = lists[i][j];
        TRITONSORT_ASSERT(list->getSize() == 0, "Found a non-empty list while "
               "destructing");
        delete list;
        lists[i][j] = NULL;
      }
      delete[] lists[i];
      lists[i] = NULL;
    }
    delete[] lists;
    lists = NULL;

    delete[] largestLists;
  }

  /// Print the sizes of each list in the table
  void printListSizes() {
    uint64_t sum = 0;

    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      uint64_t currentNumLogicalDisks = numLogicalDisksForPhysicalDisk[i];

      for (uint64_t j = 0; j < currentNumLogicalDisks; j++) {
        uint64_t curSize = lists[i][j]->getSize();
        StatusPrinter::add("List %llu,%llu: %llu", i, j, curSize);
      }
    }
  }

  /// Get the total number of buffers contained in all the table's lists
  /**
     \return the total number of buffers contained in all the table's lists
   */
  uint64_t getTotalNumBuffersInTable() const {
    uint64_t totalBuffers = 0;

    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      uint64_t currentNumLogicalDisks = numLogicalDisksForPhysicalDisk[i];

      for (uint64_t j = 0; j < currentNumLogicalDisks; j++) {
        totalBuffers += lists[i][j]->getSize();
      }
    }

    return totalBuffers;
  }

  /// Insert a buffer into the table
  /**
     Looks up the buffer's logical disk ID with BaseBuffer::getLogicalDiskID
     and uses that logical disk ID to determine what list the buffer belongs in

     \param buffer the buffer to insert
   */
  void insert(T* buffer) {
    uint64_t partition = buffer->getLogicalDiskID();

    // Compute the local disk ID for the partition
    uint64_t localDiskID =
      (partition / partitionsPerDisk) % numIntermediateDisks;
    // Adjust the disk ID to be 0-indexed from the first disk this table owns
    localDiskID = adjustPhysicalDiskID(localDiskID);

    // Adjust the partition to be 0-indexed from the disk it belongs to
    partition = partition % partitionsPerDisk;

    // Retrieve the list from our arrays
    BufferList<T>* list = lists[localDiskID][partition];

    list->append(buffer);

    if (listIsBigger(list, largestLists[localDiskID])) {
      largestLists[localDiskID] = list;
    }
  }

  /// Update cached information about the largest list for a given physical disk
  /**
     This should be called whenever the sizes of the lists change to make sure
     that cached information is consistent with the reality of the lists
     themselves

     \param physicalDiskID the physical disk ID of the disk whose cache
     information is to be updated
   */
  void updateLargestList(uint64_t physicalDiskID) {
    physicalDiskID = adjustPhysicalDiskID(physicalDiskID);

    BufferList<T>* biggestList = NULL;

    uint64_t currentNumLogicalDisks =
      numLogicalDisksForPhysicalDisk[physicalDiskID];

    for (uint64_t i = 0; i < currentNumLogicalDisks; i++) {
      BufferList<T>* list = lists[physicalDiskID][i];

      if (list->getTotalDataSize() > 0 && listIsBigger(list, biggestList)) {
        biggestList = list;
      }
    }

    largestLists[physicalDiskID] = biggestList;
  }

  /**
     This relies on the cache information updated by updateLargestList to be
     kept up-to-date

     \param physicalDiskID the physical disk ID whose largest list will be
     retrieved

     \return the largest list for the given physical disk ID
   */
  BufferList<T>* getLargestListForDisk(uint64_t physicalDiskID) {
    return largestLists[adjustPhysicalDiskID(physicalDiskID)];
  }

  /**
     \param diskSet a set of physical disk IDs

     \return the largest list for any of the disks in the provided physical
     disk ID set
   */
  BufferList<T>* getLargestListForDiskSet(std::set<uint64_t>& diskSet) {
    BufferList<T>* largestList = NULL;

    for (std::set<uint64_t>::iterator iter = diskSet.begin();
         iter != diskSet.end(); iter++) {
      uint64_t physicalDiskID = adjustPhysicalDiskID(*iter);

      BufferList<T>* currentList = largestLists[physicalDiskID];

      if (listIsBigger(currentList, largestList)) {
        largestList = currentList;
      }
    }

    return largestList;
  }

  /**
     \return the largest list in the table
   */
  BufferList<T>* getLargestList() const {
    BufferList<T>* largestList = NULL;

    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      if (listIsBigger(largestLists[i], largestList)) {
        largestList = largestLists[i];
      }
    }

    return largestList;
  }

  /// Get a set of physical disks whose lists have some minimum amount of bytes
  /// in them
  /**
     \param minimumSize the least number of bytes a list must have in it
     to be considered as a candidate list

     \param[out] diskSet a set that will be populated with physical disk IDs of
     all disks that have at least one list above the minimum size threshold
   */
  void getPhysicalDisksWithListsAboveMinimumSize(uint64_t minimumSize,
                                                 std::set<uint64_t>& diskSet) {
    for (uint64_t i = 0; i < numPhysicalDisks; i++) {
      BufferList<T>* currList = largestLists[i];
      if (currList != NULL && currList->getTotalDataSize() >= minimumSize) {
        diskSet.insert(basePhysicalDiskID + i);
      }
    }
  }

private:
  // The lowest-numbered physical disk that this table supports; used to split
  // buffer tables among groups of physical disks.
  const uint64_t basePhysicalDiskID;

  // The number of physical disks whose lists are stored in the table.
  const uint64_t numPhysicalDisks;

  // The number of partitions on each disk
  const uint64_t partitionsPerDisk;

  // The number of intermediate disks on each node
  const uint64_t numIntermediateDisks;

  // The number of logical disks per physical disk for each physical disk
  uint64_t* numLogicalDisksForPhysicalDisk;

  // First keyed by physical disk, then logical disk
  BufferList<T>*** lists;
  BufferList<T>** largestLists;

  uint64_t adjustPhysicalDiskID(uint64_t physicalDiskID) {
    TRITONSORT_ASSERT(physicalDiskID >= basePhysicalDiskID, "Physical disk ID too small");
    uint64_t adjustedID = physicalDiskID - basePhysicalDiskID;
    TRITONSORT_ASSERT(adjustedID < numPhysicalDisks, "Physical disk ID too large");
    return adjustedID;
  }

  inline bool listIsBigger(
    BufferList<T>* myList, BufferList<T>* biggestList) const {

    return myList != NULL &&
      (biggestList == NULL || biggestList->getTotalDataSize() <
       myList->getTotalDataSize());
  }
};

#endif // THEMIS_BUFFER_TABLE_H
