#ifndef MAPRED_BASE_WRITER_H
#define MAPRED_BASE_WRITER_H

#include <map>
#include <stdint.h>
#include <string>

#include "core/File.h"
#include "mapreduce/common/PartitionMap.h"

class ChunkMap;
class CoordinatorClientInterface;
class KVPairBuffer;
class NamedObjectCollection;
class Params;
class StatLogger;
class WriteTokenPool;

typedef std::map<uint64_t, std::string> OutputDiskMap;

/**
   BaseWriter encapsulates all of the writer logic that is common to both
   synchronous and asynchronous writer implementations. It performs logical disk
   ID checking, file retrieval and close operations, write size logging, and all
   required parameter checking.

   The typical usage pattern for a BaseWriter is:

   File* file = getFile(buffer);
   // perform some write operation on file...

   // After write completes...
   logBufferWritten(buffer);
 */
class BaseWriter {
public:
  /**
     Static constructor method that creates a BaseWriter based on the parameters
     passed to a worker's newInstance() method.

     \param phaseName the name of the phase

     \param stageName the name of the writer stage

     \param id the id of the writer worker

     \param params the global Params object for the application

     \param dependencies the injected dependencies for this stage

     \param fileMode open mode for file - should be either WRITE, WRITE_LIBAIO
     or WRITE_POSIXAIO

     \param logger a StatLogger created by the parent stage for logging write
     sizes

     \return a new BaseWriter object
   */
  static BaseWriter* newBaseWriter(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, NamedObjectCollection& dependencies,
    File::AccessMode fileMode, StatLogger& logger);

  /// Constructor
  /**
     \param id the id of the writer stage

     \param nodeIPAddress the IP address of this node used to inform the
     coordinator of a simulated failure

     \param writeTokenPool a write token pool where tokens will be returned

     \param fileMode open mode for file - should be either WRITE, WRITE_LIBAIO
     or WRITE_POSIXAIO

     \param directIO true if O_DIRECT should be enabled

     \param logicalDiskSizeHint how large preallocated logical disks should be

     \param outputDisks the mapping from physical disk ID to output disk that
     this writer is responsible for

     \param coordinatorClient the coordinatorClient to inform of a simulated
     failure

     \param bytesBeforeSimulatedFailure the number of bytes to write before
     simulating failure, or 0 if no failure simulation should be used

     \param logger the stat logger associated with the writer stage

     \param params the global params object

     \param numDisks the number of output disks per node

     \param phaseName the name of the phase

     \param largePartitionThreshold files larger than this threshold in bytes
     will be flagged as large partitions

     \param chunkMap the global chunk map

     \param nodeID the ID of this node in the cluster
   */
  BaseWriter(
    uint64_t id,  const std::string& nodeIPAddress,
    WriteTokenPool* writeTokenPool, File::AccessMode fileMode, bool directIO,
    uint64_t logicalDiskSizeHint, OutputDiskMap& outputDisks,
    CoordinatorClientInterface& coordinatorClient,
    uint64_t bytesBeforeSimulatedFailure, StatLogger& logger,
    const Params& params, uint64_t numDisks,
    const std::string& phaseName, uint64_t largePartitionThreshold,
    ChunkMap* chunkMap, uint64_t nodeID);

  /// Destructor
  virtual ~BaseWriter();

  /**
     Retrieve the file that a given buffer should be written to. Automatically
     opens the file with the proper mode.

     \param writeBuffer the buffer to write

     \return the file to write to
   */
  File* getFile(KVPairBuffer* writeBuffer);

  /**
     Logs that a buffer has been written and performs failure simulation if
     specified. Automatically returns write tokens to the pool, but does NOT
     delete the buffer.

     \param writeBuffer the buffer to log a completed write for
   */
  void logBufferWritten(KVPairBuffer* writeBuffer);

  /**
     Performs teardown operations including closing files and logging the number
     of bytes written.
   */
  void teardown();

private:
  typedef std::map<uint64_t, File*> FileMap;
  typedef std::map<uint64_t, FileMap> FilesForJobMap;

  // Kind of an abuse of notation but oh well.
  typedef std::map<uint64_t, FilesForJobMap> ChunkFilesForJobMap;

  typedef std::map<uint64_t, bool> BooleanMap;

  /**
     Helper function that opens a new file.

     \param logicalDiskUID the id of the logical disk for the file

     \param jobID the job that this file belongs to

     \param outputDisk the disk path where the file should be stored

     \param chunkID the ID of a large chunk in phase three

     \param replica if true, this file is a replica file

     \return a new file to write to
   */
  File* newFile(
    uint64_t logicalDiskUID, uint64_t jobID, const std::string& outputDisk,
    uint64_t chunkID, bool replica);

  const uint64_t id;
  const std::string nodeIPAddress;
  const File::AccessMode fileMode;
  const bool directIO;
  const uint64_t logicalDiskSizeHint;
  const OutputDiskMap outputDisks;
  const uint64_t bytesBeforeSimulatedFailure;
  const uint64_t numDisks;
  const uint64_t largePartitionThreshold;
  const uint64_t nodeID;

  BooleanMap blackHoleWrites;

  CoordinatorClientInterface& coordinatorClient;

  PartitionMap partitionMap;

  WriteTokenPool* writeTokenPool;

  FilesForJobMap files;
  ChunkFilesForJobMap chunkFiles;

  ChunkMap* chunkMap;

  StatLogger& logger;
  uint64_t writeSizeStatID;
  uint64_t totalBytesWritten;
};

#endif // MAPRED_BASE_WRITER_H
