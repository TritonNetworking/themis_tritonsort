#include <sstream>
#include <sys/mman.h>

#include "core/File.h"
#include "core/MemoryUtils.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/DiskBackedBoundaryKeyList.h"
#include "mapreduce/common/boundary/PartitionBoundaries.h"

std::string DiskBackedBoundaryKeyList::getBoundaryKeyListFilename(
  const Params& params, uint64_t jobID) {

  std::ostringstream oss;
  oss << "DISK_BACKED_BOUNDARY_LIST." << jobID;

  return params.get<std::string>(oss.str());
}

DiskBackedBoundaryKeyList* DiskBackedBoundaryKeyList::newBoundaryKeyList(
  const Params& params, uint64_t jobID, uint64_t numPartitions) {

  std::string filename(getBoundaryKeyListFilename(params, jobID));

  ABORT_IF(fileExists(filename), "File '%s' already exists", filename.c_str());

  // Write metadata to the file
  File partitionFile(filename);

  BoundaryKeyInfo invalidInfo;
  memset(&invalidInfo, 0, sizeof(BoundaryKeyInfo));
  invalidInfo.valid = false;
  invalidInfo.offset = 0;
  invalidInfo.length = 0;

  partitionFile.open(File::WRITE, true);

  partitionFile.write(
    reinterpret_cast<uint8_t*>(&numPartitions), sizeof(uint64_t));

  for (uint64_t i = 0; i < numPartitions; i++) {
    partitionFile.write(
      reinterpret_cast<uint8_t*>(&invalidInfo), sizeof(BoundaryKeyInfo));
  }

  partitionFile.sync();
  partitionFile.close();

  // Pass the file whose metadata you just initialized back into the
  // constructor for a new disk-backed boundary list
  return new DiskBackedBoundaryKeyList(filename);
}

DiskBackedBoundaryKeyList* DiskBackedBoundaryKeyList::loadForJob(
  const Params& params, uint64_t jobID) {

  std::string filename(getBoundaryKeyListFilename(params, jobID));

  return new DiskBackedBoundaryKeyList(filename);
}

DiskBackedBoundaryKeyList::DiskBackedBoundaryKeyList(
  const std::string& filename)
  : partitionFile(filename),
    mmapMetadata(NULL),
    metadataSize(0),
    numPartitions(0),
    partitionBoundaryKeys(NULL),
    nextFreePartition(0),
    nextFreeOffset(0) {

  partitionFile.open(File::READ_WRITE);

  // Read the number of partitions from disk
  partitionFile.read(
    reinterpret_cast<uint8_t*>(&numPartitions), sizeof(uint64_t));

  // Seek the file back to the beginning so that we can re-read
  partitionFile.seek(0, File::FROM_BEGINNING);

  metadataSize = (numPartitions * sizeof(BoundaryKeyInfo)) + sizeof(uint64_t);

  // Load metadata (incl. number of partitions since mmap requires that file
  // offset be a multiple of page size)
  mmapMetadata = mmap(
    NULL, metadataSize, PROT_READ|PROT_WRITE, MAP_SHARED,
    partitionFile.getFileDescriptor(), 0);

  // Point offsets at the appropriate offset in the mmap'd region
  partitionBoundaryKeys = reinterpret_cast<BoundaryKeyInfo*>(
    reinterpret_cast<uint8_t*>(mmapMetadata) + sizeof(uint64_t));

  // Scan through the partition keys for this file and find the first invalid
  // one; this is where we can start adding new partitions. As you're checking,
  // make sure that the metadata is self-consistent (as lack of that
  // self-consistency indicates corruption)

  uint64_t currentOffset = metadataSize;

  bool foundNextPartition = false;
  for (uint64_t i = 0; i < numPartitions; i++) {
    BoundaryKeyInfo& boundaryKeyInfo = partitionBoundaryKeys[i];

    if (!boundaryKeyInfo.valid) {
      if (!foundNextPartition) {
        nextFreePartition = i;
        nextFreeOffset = currentOffset;
        foundNextPartition = true;
      }
    } else {
      ABORT_IF(
        foundNextPartition, "Expected a single, continuous region of valid "
        "BoundaryKeyInfos, but found an valid BoundaryKeyInfo among invalid "
        "BoundaryKeyInfos at index %llu", i);

      ABORT_IF(boundaryKeyInfo.offset != currentOffset, "Corrupted "
               "offset detected at partition %llu", i);
      currentOffset += boundaryKeyInfo.length;
    }
  }

  if (!foundNextPartition) {
    nextFreePartition = numPartitions;
    nextFreeOffset = currentOffset;
  }
}

DiskBackedBoundaryKeyList::~DiskBackedBoundaryKeyList() {
  if (mmapMetadata != NULL) {
    msync(mmapMetadata, metadataSize, MS_SYNC);
    munmap(mmapMetadata, metadataSize);
    mmapMetadata = NULL;
  }

  // Don't have to delete offsets, since it's just a pointer to a subsection of
  // mmapMetadata
  partitionBoundaryKeys = NULL;

  partitionFile.sync();
  partitionFile.close();
}

void DiskBackedBoundaryKeyList::addBoundaryKey(
  const uint8_t* key, uint32_t keyLength) {

  ABORT_IF(nextFreePartition >= numPartitions, "Can't add more boundary keys "
           "to a file that is already full of keys");

  // Figure out where to write the partition key

  uint64_t partitionNumber = nextFreePartition++;

  // If this is the first partition, it should be written immediately after the
  // metadata
  uint64_t fileOffset = metadataSize;

  if (partitionNumber > 0) {
    BoundaryKeyInfo& previousPartitionKeyInfo =
      partitionBoundaryKeys[partitionNumber - 1];

    TRITONSORT_ASSERT(previousPartitionKeyInfo.valid,"Expected partition %llu to be "
           "valid, but it wasn't", partitionNumber - 1);

    fileOffset = previousPartitionKeyInfo.offset +
      previousPartitionKeyInfo.length;
  }

  // Write the partition key to the appropriate offset in the file
  partitionFile.seek(fileOffset, File::FROM_BEGINNING);
  partitionFile.write(key, keyLength);

  // Update the partition's metadata
  BoundaryKeyInfo& partitionKeyInfo = partitionBoundaryKeys[partitionNumber];

  ABORT_IF(partitionKeyInfo.valid, "Can't mutate existing boundary keys "
           "(boundary key at index %llu was found to be valid, expected it to "
           "be invalid)", partitionNumber);
  partitionKeyInfo.offset = fileOffset;
  partitionKeyInfo.length = keyLength;
  partitionKeyInfo.valid = true;
}

PartitionBoundaries* DiskBackedBoundaryKeyList::getPartitionBoundaries(
  uint64_t partitionNumber) {

  return getPartitionBoundaries(partitionNumber, partitionNumber);
}

PartitionBoundaries* DiskBackedBoundaryKeyList::getPartitionBoundaries(
  uint64_t startPartition, uint64_t endPartition) {
  ABORT_IF(endPartition < startPartition, "Start partition (%llu) must be <= "
           "end partition (%llu)", startPartition, endPartition);
  ABORT_IF(startPartition >= numPartitions, "Partition %llu out-of-bounds "
           "(should be in range [0, %llu))", startPartition, numPartitions);
  ABORT_IF(endPartition >= numPartitions, "Partition %llu out-of-bounds "
           "(should be in range [0, %llu))", endPartition, numPartitions);

  uint8_t* startKey = NULL;
  uint32_t startKeyLength = 0;
  uint8_t* endKey = NULL;
  uint32_t endKeyLength = 0;

  getBoundaryKey(startPartition, startKey, startKeyLength);

  if ((endPartition + 1) < numPartitions) {
    getBoundaryKey((endPartition + 1), endKey, endKeyLength);
  }

  return new PartitionBoundaries(
    startKey, startKeyLength, endKey, endKeyLength);
}


void DiskBackedBoundaryKeyList::getBoundaryKey(
  uint64_t partitionNumber, uint8_t*& key, uint32_t& keyLength) {

  BoundaryKeyInfo& partitionInfo = partitionBoundaryKeys[partitionNumber];
  ABORT_IF(!partitionInfo.valid, "Don't have boundary information for "
           "partition %llu", partitionNumber);
  keyLength = partitionInfo.length;

  partitionFile.seek(partitionInfo.offset, File::FROM_BEGINNING);

  key = new (themis::memcheck) uint8_t[keyLength];

  partitionFile.read(key, keyLength);
}
