#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/ChunkMap.h"

ChunkMap::ChunkMap(uint64_t _disksPerNode)
  : disksPerNode(_disksPerNode),
    nextDiskID(0) {
  pthread_mutex_init(&lock, NULL);
}

ChunkMap::~ChunkMap() {
  pthread_mutex_destroy(&lock);
}

uint64_t ChunkMap::addChunk(uint64_t partitionID, uint64_t chunkSize) {
  ScopedLock scopedLock(&lock);
  uint64_t numChunks = diskMap[partitionID].size();
  diskMap[partitionID][numChunks] = nextDiskID;
  sizeMap[partitionID][numChunks] = chunkSize;
  nextDiskID = (nextDiskID + 1) % disksPerNode;

  return numChunks;
}

uint64_t ChunkMap::getDiskID(uint64_t partitionID, uint64_t chunkID) {
  ScopedLock scopedLock(&lock);
  DiskMap::iterator iter = diskMap.find(partitionID);
  ABORT_IF(iter == diskMap.end(),
           "Could not find partition ID %llu", partitionID);
  ChunkToDiskMap::iterator iter2 = (iter->second).find(chunkID);
  ABORT_IF(iter == diskMap.end(),
           "Could not find chunk ID %llu for partition ID %llu",
           chunkID, partitionID);

  return iter2->second;
}

const ChunkMap::DiskMap& ChunkMap::getDiskMap() {
  return diskMap;
}

const ChunkMap::SizeMap& ChunkMap::getSizeMap() {
  return sizeMap;
}
