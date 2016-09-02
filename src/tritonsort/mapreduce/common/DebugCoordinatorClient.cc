#include "common/MainUtils.h"
#include "core/Glob.h"
#include "core/Params.h"
#include "mapreduce/common/DebugCoordinatorClient.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/ReadRequest.h"

DebugCoordinatorClient::DebugCoordinatorClient(
  const Params& params, const std::string& phaseName, uint64_t _diskID)
  : numPartitions(params.get<uint64_t>("NUM_PARTITIONS")),
    diskID(_diskID),
    outputDirectory("local:///job_0") {
  // Pull our disk out of the input disk list.
  StringList disks;
  std::string inputDiskListParam = "INPUT_DISK_LIST." + phaseName;
  getDiskList(disks, inputDiskListParam, &params);
  uint64_t diskCount = 0;
  for (StringList::iterator iter = disks.begin(); iter != disks.end();
       iter++, diskCount++) {
    if (diskCount == diskID) {
      // Use the disk name for another glob to get the file list.
      Glob fileGlob(*iter + "/job_*/*.partition");
      files = fileGlob.getFiles();
      // Set the iterator to the beginning of the file list.
      nextFile = files.begin();
      break;
    }
  }
}

ReadRequest* DebugCoordinatorClient::getNextReadRequest() {
  if (nextFile == files.end()) {
    // Repeated calls on this function after all files have been globbed should
    // return NULL.
    return NULL;
  }

  ReadRequest* readRequest = new ReadRequest(*nextFile, diskID);
  readRequest->jobIDs.insert(0);
  nextFile++;

  return readRequest;
}

JobInfo* DebugCoordinatorClient::getJobInfo(uint64_t jobID) {
  // There's only one job in debug mode, and its properties are specified in
  // params so load them.
  JobInfo* jobInfo = new JobInfo(
    jobID, "", "", "", "", "", "", 0, numPartitions);
  return jobInfo;
}

const themis::URL& DebugCoordinatorClient::getOutputDirectory(uint64_t jobID) {
  return outputDirectory;
}

RecoveryInfo* DebugCoordinatorClient::getRecoveryInfo(uint64_t jobID) {
  // Recovery not supported in debug mode.
  return NULL;
}

void DebugCoordinatorClient::notifyNodeFailure(
  const std::string& peerIPAddress) {
  // Failure notification not supported in debug mode.
  ABORT("Node failure");
}

void DebugCoordinatorClient::notifyDiskFailure(
  const std::string& peerIPAddress, const std::string& diskPath) {
  // Failure notification not supported in debug mode.
  ABORT("Disk failure");
}

void DebugCoordinatorClient::setNumPartitions(
  uint64_t jobID, uint64_t numPartitions) {
  // Setting number of partitions not supported in debug mode.
}

void DebugCoordinatorClient::waitOnBarrier(const std::string& barrierName) {
  // Barriers not supported in debug mode.
  ABORT("Barrier");
}

void DebugCoordinatorClient::uploadSampleStatistics(
  uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes) {
}

void DebugCoordinatorClient::getSampleStatisticsSums(
  uint64_t jobId, uint64_t numNodes, uint64_t& inputBytes,
  uint64_t& intermediateBytes) {
}

uint64_t DebugCoordinatorClient::getNumPartitions(uint64_t jobID) {
  return 0;
}
