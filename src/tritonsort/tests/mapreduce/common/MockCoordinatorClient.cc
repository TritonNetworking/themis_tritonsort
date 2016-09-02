#include "tests/mapreduce/common/MockCoordinatorClient.h"

#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/RecoveryInfo.h"

MockCoordinatorClient::MockCoordinatorClient()
  : outputURL("local:///") {

}

MockCoordinatorClient::~MockCoordinatorClient() {
  for (JobInfoMap::iterator iter = jobInfos.begin(); iter != jobInfos.end();
       iter++) {
    delete iter->second;
  }

  jobInfos.clear();

  for (RecoveryInfoMap::iterator iter = recoveryInfos.begin();
       iter != recoveryInfos.end(); iter++) {
    delete iter->second;
  }

  recoveryInfos.clear();
}

ReadRequest* MockCoordinatorClient::getNextReadRequest() {
  if (readRequests.size() == 0) {
    return NULL;
  } else {
    ReadRequest* readRequest = readRequests.front();
    readRequests.pop_front();
    return readRequest;
  }
}

void MockCoordinatorClient::addReadRequest(
  ReadRequest* readRequest) {
  readRequests.push_back(readRequest);
}

JobInfo* MockCoordinatorClient::getJobInfo(uint64_t jobID) {
  JobInfoMap::iterator iter = jobInfos.find(jobID);

  if (iter != jobInfos.end()) {
    return iter->second;
  } else {
    return NULL;
  }
}

const themis::URL& MockCoordinatorClient::getOutputDirectory(uint64_t jobID) {
  return outputURL;
}

void MockCoordinatorClient::setJobInfo(uint64_t jobID, JobInfo* jobInfo) {
  jobInfos[jobID] = jobInfo;
}

RecoveryInfo* MockCoordinatorClient::getRecoveryInfo(uint64_t jobID) {
  RecoveryInfoMap::iterator iter = recoveryInfos.find(jobID);

  if (iter != recoveryInfos.end()) {
    RecoveryInfo* recoveryInfo = iter->second;

    RecoveryInfo* copy = new RecoveryInfo(
      recoveryInfo->recoveringJob, recoveryInfo->partitionRangesToRecover);

    return copy;
  } else {
    return NULL;
  }
}

void MockCoordinatorClient::setRecoveryInfo(
  uint64_t jobID, RecoveryInfo* recoveryInfo) {
  recoveryInfos[jobID] = recoveryInfo;
}


void MockCoordinatorClient::notifyNodeFailure(
  const std::string& peerIPAddress) {
  // No-op
}

void MockCoordinatorClient::notifyDiskFailure(
  const std::string& peerIPAddress, const std::string& diskPath) {
  // No-op
}

void MockCoordinatorClient::setNumPartitions(
  uint64_t jobID, uint64_t numPartitions) {
  // No-op
}

void MockCoordinatorClient::waitOnBarrier(const std::string& barrierName) {
  // No-op
}

void MockCoordinatorClient::uploadSampleStatistics(
  uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes) {
}

void MockCoordinatorClient::getSampleStatisticsSums(
  uint64_t jobId, uint64_t numNodes, uint64_t& inputBytes,
  uint64_t& intermediateBytes) {
}

uint64_t MockCoordinatorClient::getNumPartitions(uint64_t jobID) {
  return 0;
}
