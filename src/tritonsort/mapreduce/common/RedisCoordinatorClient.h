#ifndef MAPRED_REDIS_COORDINATOR_CLIENT_H
#define MAPRED_REDIS_COORDINATOR_CLIENT_H

#include <hiredis/hiredis.h>
#include <json/json.h>
#include <set>

#include "mapreduce/common/CoordinatorClientInterface.h"

class Params;

/**
   Communicates with the cluster coordinator by reading and writing to a
   objects in a Redis database
 */
class RedisCoordinatorClient : public CoordinatorClientInterface {
public:
  /// Constructor
  /**
     \param params the Params object from which the client will retrieve
     information about the job and redis timeout parameters

     \param phaseName the name of the phase

     \param role the client's "role" (typically its stage name, but can be
     arbitrary)

     \params id the client's unique ID within its role
   */
  RedisCoordinatorClient(
    const Params& params, const std::string& phaseName, const std::string& role,
    uint64_t id);

  /// Destructor
  virtual ~RedisCoordinatorClient();

  /// Get the next read request from Redis queue
  /// "read_requests:<MY_IP_ADDRESS>:<role>:<id>"
  ReadRequest* getNextReadRequest();

  /// \sa CoordinatorClientInterface::getJobInfo
  JobInfo* getJobInfo(uint64_t jobID);

  /// \sa CoordinatorClientInterface::getOutputDirectory
  const URL& getOutputDirectory(uint64_t jobID);

  RecoveryInfo* getRecoveryInfo(uint64_t jobID);

  void notifyNodeFailure(const std::string& peerIPAddress);

  void notifyDiskFailure(
    const std::string& peerIPAddress, const std::string& diskPath);

  void setNumPartitions(uint64_t jobID, uint64_t numPartitions);

  void waitOnBarrier(const std::string& barrierName);

  void uploadSampleStatistics(
    uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes);

  void getSampleStatisticsSums(
    uint64_t jobId, uint64_t numNodes, uint64_t& inputBytes,
    uint64_t& intermediateBytes);

  uint64_t getNumPartitions(uint64_t jobID);

private:
  typedef std::map<uint64_t, URL> OutputDirectoryMap;

  ReadRequest* parseReadRequest(redisReply& reply, bool& halt);

  bool jobIDSetMatchesExpected(
    Json::Value* requestJobIDs, std::set<uint64_t>& requestJobIDSet);

  void notifyFailure(
    const std::string& peerIPAddress, const std::string* diskPath);

  const std::string phaseName;
  const std::string role;
  const std::string address;
  const uint64_t id;
  const uint64_t popTimeout;
  const uint64_t batchID;
  const uint64_t numNodes;
  const uint64_t redisPollInterval;

  char* readRequestPopCommand;

  Json::Reader jsonReader;

  std::set<uint64_t> currentBatchJobIDs;

  OutputDirectoryMap outputDirectories;
};

#endif // MAPRED_REDIS_COORDINATOR_CLIENT_H
