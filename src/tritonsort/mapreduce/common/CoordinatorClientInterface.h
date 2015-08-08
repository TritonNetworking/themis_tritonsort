#ifndef MAPRED_COORDINATOR_CLIENT_INTERFACE_H
#define MAPRED_COORDINATOR_CLIENT_INTERFACE_H

#include "mapreduce/common/URL.h"
#include <stdint.h>
#include <string>

class JobInfo;
class ReadRequest;
class RecoveryInfo;

/**
   The interface that all coordinator clients are expected to support
 */
class CoordinatorClientInterface
{
public:
  /**
     If this client is connected to the coordinator, the destructor will
     disconnect from it
   */
  virtual ~CoordinatorClientInterface() {}

  /**
     Block until there is a read request available for you, or until the
     coordinator wants you to halt.

     \return a pointer to a new ReadRequest object containing information about
     the request, or NULL if the coordinator wants you to halt
   */
  virtual ReadRequest* getNextReadRequest() = 0;

  /**
     Get information about a job with a given ID
   */
  virtual JobInfo* getJobInfo(uint64_t jobID) = 0;

  /**
     Get the directory that writers should use to write output files for a
     particular job.
   */
  virtual const URL& getOutputDirectory(uint64_t jobID) = 0;

  /**
     Get information about the recovery operation that a job is performing

     \return NULL if the job is not recovering anything, or information about
     the recovery
   */
  virtual RecoveryInfo* getRecoveryInfo(uint64_t jobID) = 0;

  /**
     Notify the coordinator that we believe a node has failed

     \param peerIPAddress the IP address of the failed node
   */
  virtual void notifyNodeFailure(const std::string& peerIPAddress) = 0;

  /**
     Notify the coordinator that one of our intermediate disks has failed

     \param peerIPAddress the IP address of the failed node

     \param diskPath the intermediate disk path that failed (should be a member
     of the INTERMEDIATE_DISK_LIST or OUTPUT_DISK_LIST parameters' disk list)
   */
  virtual void notifyDiskFailure(
    const std::string& peerIPAddress, const std::string& diskPath) = 0;

  /**
     Store the number of partitions associated with a job in redis. This value
     will be loaded in subsequent getJobInfo() calls.

     \param jobID the running job

     \param numPartitions the number of partitions for the job
   */
  virtual void setNumPartitions(uint64_t jobID, uint64_t numPartitions) = 0;

  /**
     Wait on a distributed barrier by blocking until all nodes in the cluster
     execute this function.

     \param barrierName the name of the barrier
   */
  virtual void waitOnBarrier(const std::string& barrierName) = 0;

  /**
     Upload the sampled input and intermediate bytes to the coordinator.

     \param jobID the running job

     \param inputBytes the number of input bytes sampled

     \param intermediateBytes the number of mapper output bytes for the sample
   */
  virtual void uploadSampleStatistics(
    uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes) = 0;

  /**
     Collect all sampled input and intermediate bytes from the cluster.

     \param jobID the running job

     \param numNodes the number of nodes in the cluster

     \param[out] inputBytes input byte sample statistics will be stored in this
     vector

     \param[out] intermediateBytes intermediate byte sample statistics will be
     stored in this vector
   */
  virtual void getSampleStatisticsSums(
    uint64_t jobId, uint64_t numNodes, uint64_t& totalInputBytes,
    uint64_t& totalIntermediateBytes) = 0;

  /**
     Wait until the number of partitions for a job has been set and then return
     it.

     \param jobID the running job

     \return the number of partitions for this job
   */
  virtual uint64_t getNumPartitions(uint64_t jobID) = 0;

  /// All coordinator messages have a type, defined by their "type" field. This
  /// makes it easier for the client to easily discern the kind of message that
  /// it's looking at. The static uint64_ts below define those type numbers.

  const static uint64_t COORDINATOR_READ_REQUEST_TYPE = 0;
  const static uint64_t COORDINATOR_HALT_REQUEST_TYPE = 1;
};

#endif // MAPRED_COORDINATOR_CLIENT_INTERFACE_H
