#ifndef THEMIS_MOCK_COORDINATOR_CLIENT_H
#define THEMIS_MOCK_COORDINATOR_CLIENT_H

#include <list>
#include <map>

#include "mapreduce/common/CoordinatorClientInterface.h"

class JobInfo;
class ReadRequest;
class RecoveryInfo;

/**
   A mock coordinator client that can be injected with the various kinds of
   data structures that normal coordinator clients expect to return, so that we
   can test interactions with the coordinator without actually setting one up
 */
class MockCoordinatorClient : public CoordinatorClientInterface {
public:
  /// Constructor
  MockCoordinatorClient();

  /// Destructor
  virtual ~MockCoordinatorClient();

  ReadRequest* getNextReadRequest();

  /// Add readRequest to the read request queue
  void addReadRequest(ReadRequest* readRequest);

  JobInfo* getJobInfo(uint64_t jobID);

  const URL& getOutputDirectory(uint64_t jobID);

  /// Associate the JobInfo object jobInfo with job jobID
  void setJobInfo(uint64_t jobID, JobInfo* jobInfo);

  RecoveryInfo* getRecoveryInfo(uint64_t jobID);

  /// Associate the RecoveryInfo object recoveryInfo with job jobID
  void setRecoveryInfo(uint64_t jobID, RecoveryInfo* recoveryInfo);

  void notifyNodeFailure(const std::string& peerIPAddress);

  void notifyDiskFailure(
    const std::string& peerIPAddress, const std::string& diskPath);

  void setNumPartitions(uint64_t jobID, uint64_t numPartitions);

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
  typedef std::map<uint64_t, JobInfo*> JobInfoMap;
  typedef std::map<uint64_t, RecoveryInfo*> RecoveryInfoMap;
  typedef std::list<ReadRequest*> ReadRequestList;

  JobInfoMap jobInfos;
  RecoveryInfoMap recoveryInfos;
  ReadRequestList readRequests;
  URL outputURL;
};


#endif // THEMIS_MOCK_COORDINATOR_CLIENT_H
