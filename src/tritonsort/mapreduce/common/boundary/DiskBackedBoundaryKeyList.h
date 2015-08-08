#ifndef THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_H
#define THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_H

#include <stdint.h>
#include <string>

#include "core/constants.h"
#include "core/File.h"

class Params;
class PartitionBoundaries;

/**
   A list of all the boundary keys for a job's partitions, stored in an on-disk
   file
 */
class DiskBackedBoundaryKeyList {
public:
  /// Construct a new, empty disk-backed boundary key list for the given job ID
  /// and number of partitions
  /**
     The location of the file to be written is given by the
     DISK_BACKED_BOUNDARY_LIST.<jobID> parameter.

     \param params a Params object from which the filename that will back this
     key list will be retrieved

     \param jobID the job ID for the job whose boundary keys will be stored

     \param numPartitions the number of partitions that will be stored

     \return a pointer to the newly-created DiskBackedBoundaryKeyList. Caller
     is responsible for destructing.
   */
  static DiskBackedBoundaryKeyList* newBoundaryKeyList(
    const Params& params, uint64_t jobID, uint64_t numPartitions);

  /// Load a disk-backed boundary key list from disk
  /**
     The location of the file to be written is given by the
     DISK_BACKED_BOUNDARY_LIST.<jobID> parameter.

     \param params a Params object from which the filename that will back this
     key list will be retrieved

     \param jobID the job ID for the job whose boundary keys will be stored

     \return a pointer to the newly-created DiskBackedBoundaryKeyList. Caller
     is responsible for destructing.
   */
  static DiskBackedBoundaryKeyList* loadForJob(
    const Params& params, uint64_t jobID);

  /// Destructor
  virtual ~DiskBackedBoundaryKeyList();

  /// Add a boundary key to the boundary key list
  /**
     This boundary key is added to the next partition number whose key has not
     been specified.

     \param key the boundary key to add

     \param keyLength the length of the boundary key in bytes
   */
  void addBoundaryKey(const uint8_t* key, uint32_t keyLength);

  /// Get the partition boundaries for a given partition
  /**
     \param partitionNumber the partition number of the partition whose
     boundaries are to be retrieved

     \return a new PartitionBoundaries object corresponding to the boundaries
     of the specified partition. Caller is responsible for destructing.
   */
  PartitionBoundaries* getPartitionBoundaries(uint64_t partitionNumber);

  /// Get the partition boundaries for a given range of partitions
  /**
     Produces a partition range corresponding to partitions
     [startPartition, endPartition] (inclusive on both ends)

     \param startPartition the partition that begins the partition range

     \param endPartition the partition that ends the partition range

     \return a new PartitionBoundaries object corresponding to the boundaries
     of the specified range. Caller is responsible for destructing.
   */
  PartitionBoundaries* getPartitionBoundaries(
    uint64_t startPartition, uint64_t endPartition);

private:
  DISALLOW_COPY_AND_ASSIGN(DiskBackedBoundaryKeyList);

  typedef struct {
    // Whether the boundary key has been set or not (if false, offset and
    // length are not considered valid)
    bool valid;
    // Offset of the boundary key in the file
    uint64_t offset;
    // Length of the boundary key
    uint32_t length;
  } BoundaryKeyInfo;

  static std::string getBoundaryKeyListFilename(
    const Params& params, uint64_t jobID);

  explicit DiskBackedBoundaryKeyList(const std::string& filename);

  void getBoundaryKey(
    uint64_t partitionNumber, uint8_t*& key, uint32_t& keyLength);

  File partitionFile;

  void* mmapMetadata;
  uint64_t metadataSize;

  uint64_t numPartitions;
  BoundaryKeyInfo* partitionBoundaryKeys;

  uint64_t nextFreePartition;
  uint64_t nextFreeOffset;
};


#endif // THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_H
