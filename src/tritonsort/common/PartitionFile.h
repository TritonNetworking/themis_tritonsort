#ifndef TRITONSORT_PARTITION_FILE_H
#define TRITONSORT_PARTITION_FILE_H

#include "core/File.h"

/**
   An PartitionFile differs from a File in that it is explicitly aware of
   its partition ID.
 */
class PartitionFile : public File {
public:
  /**
     Check if a filename corresponds to a partition file format.

     \param filename the file name to check

     \param partitionID[out] output parameter for partitionID

     \param jobID[out] output parameter for jobID

     \param chunkID[out] output parameter for chunkID

     \return true if the filename matches partition file format
   */
  static bool matchFilename(
    const std::string& filename, uint64_t& partitionID, uint64_t& jobID,
    uint64_t& chunkID);

  /// Constructor
  /**
     \param filename the path to the file. Assumed to be a file named
     P.partition in a directory job_J. Two alternate file name formats are also
     accepted: P.partition.large for large partitions and P.partition.chunk_C
     for chunk files in phase three.
   */
  PartitionFile(const std::string& filename);

  /**
     \return the partition ID for this file
   */
  uint64_t getPartitionID() const;

  /**
     \return the job ID for this file
   */
  uint64_t getJobID() const;
private:
  uint64_t partitionID;
  uint64_t jobID;
};

#endif // TRITONSORT_PARTITION_FILE_H
