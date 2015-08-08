#ifndef MAPRED_CHUNK_MAP_H
#define MAPRED_CHUNK_MAP_H

#include <pthread.h>

#include "core/constants.h"

/**
   ChunkMap is a data structure for determining on which disks do large
   partition chunk files belong.
 */
class ChunkMap {
public:
  typedef std::map<uint64_t, uint64_t> ChunkToDiskMap;
  typedef std::map<uint64_t, ChunkToDiskMap> DiskMap;
  typedef std::map<uint64_t, uint64_t> ChunkToSizeMap;
  typedef std::map<uint64_t, ChunkToSizeMap> SizeMap;

  /// Constructor
  /**
     \param disksPerNode the total number of disks per node
   */
  ChunkMap(uint64_t disksPerNode);

  /// Destructor
  virtual ~ChunkMap();

  /**
     Add a new chunk for a partition to the chunk map.

     \param partitionID the partition to add a chunk for

     \param the size of the chunk

     \return the ID of the new chunk
   */
  uint64_t addChunk(uint64_t partitionID, uint64_t chunkSize);

  /**
     Get the disk that a chunk belongs to.

     \param partitionID the partition we are interested in

     \param chunkID the chunk that we are interested in

     \return the disk ID that the chunk belongs to
   */
  uint64_t getDiskID(uint64_t partitionID, uint64_t chunkID);

  /**
     Get the disk map associated with this chunk map for the second part of
     phase three.

     \return the disk map
   */
  const DiskMap& getDiskMap();

  /**
     Get the size map associated with this chunk map for the second part of
     phase three.

     \return the size map
   */
  const SizeMap& getSizeMap();

private:
  const uint64_t disksPerNode;

  pthread_mutex_t lock;
  DiskMap diskMap;
  SizeMap sizeMap;
  uint64_t nextDiskID;

  DISALLOW_COPY_AND_ASSIGN(ChunkMap);
};

#endif // MAPRED_CHUNK_MAP_H
