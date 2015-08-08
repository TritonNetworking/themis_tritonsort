#ifndef MAPRED_DEBUG_COORDINATOR_CLIENT_H
#define MAPRED_DEBUG_COORDINATOR_CLIENT_H

#include "core/constants.h"
#include "mapreduce/common/CoordinatorClientInterface.h"

class Params;

/**
   DebugCoordinatorClient is a coordinator client with minimal functionality. It
   does not connect to any kind of persistent backing store, but instead draws
   information from the global params object:

   INPUT_DISK_LIST: list of input disks to read input files from. Files should
   be named *input_*

   NUM_PARTITIONS: the number of partitions, or files, for the job
 */
class DebugCoordinatorClient : public CoordinatorClientInterface {
public:
  /// Constructor
  /**
     \param params the global params object.

     \param phaseName the name of the phase

     \param diskID the ID of the disk to get read requests from
   */
  DebugCoordinatorClient(
    const Params& params, const std::string& phaseName, uint64_t diskID);

  /// Destructor
  virtual ~DebugCoordinatorClient() {}

  /**
     Get the next read request, which is generated from the disks and files in
     the specified input directory.

     \return a read request object, or NULL if all read requests have been
     retrieved
   */
  ReadRequest* getNextReadRequest();

  /**
     Get the JobInfo corresponding to the directories and partitions stored in
     params. The job ID is ignored.

     \param jobID ignored

     \return a JobInfo object corresponding to the directories and partitions
     stored in params
   */
  JobInfo* getJobInfo(uint64_t jobID);

  /// Store all outputs in the root directory of the disk.
  const URL& getOutputDirectory(uint64_t jobID);

  /// Not implemented, returns NULL.
  RecoveryInfo* getRecoveryInfo(uint64_t jobID);

  /// Not implemented, aborts
  void notifyNodeFailure(const std::string& peerIPAddress);

  /// Not implemented, aborts
  void notifyDiskFailure(
    const std::string& peerIPAddress, const std::string& diskPath);

  /// Not implemented
  void setNumPartitions(uint64_t jobID, uint64_t numPartitions);

  /// Not implemented
  void waitOnBarrier(const std::string& barrierName);

  /// Not implemented
  void uploadSampleStatistics(
    uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes);

  /// Not implemented
  void getSampleStatisticsSums(
    uint64_t jobId, uint64_t numNodes, uint64_t& inputBytes,
    uint64_t& intermediateBytes);

  /// Not implemented
  uint64_t getNumPartitions(uint64_t jobID);

private:
  const uint64_t numPartitions;
  const uint64_t diskID;

  StringList files;
  StringList::iterator nextFile;

  URL outputDirectory;
};

#endif // MAPRED_DEBUG_COORDINATOR_CLIENT_H
