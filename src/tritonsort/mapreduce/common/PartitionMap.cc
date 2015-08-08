#include "core/Params.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/PartitionMap.h"

PartitionMap::PartitionMap(const Params& params, const std::string& phaseName)
  : numNodes(params.get<uint64_t>("NUM_PEERS")),
    localNodeID(params.get<uint64_t>("MYPEERID")),
    numPartitionGroups(params.get<uint64_t>("NUM_PARTITION_GROUPS")),
    totalNumOutputDisks(
      numNodes * params.getv<uint64_t>(
        "NUM_OUTPUT_DISKS.%s", phaseName.c_str())),
    coordinatorClient(NULL) {
  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, phaseName, "partition_map", 0);
}

PartitionMap::~PartitionMap() {
  if (coordinatorClient != NULL) {
    delete coordinatorClient;
    coordinatorClient = NULL;
  }
}

uint64_t PartitionMap::getNumPartitions(uint64_t jobID) {
  InternalCountsMap::iterator iter = partitionMap.find(jobID);

  if (iter != partitionMap.end()) {
    return iter->second;
  } else {
    // Get the number of partitions from the coordinator.
    JobInfo* jobInfo = coordinatorClient->getJobInfo(jobID);
    uint64_t numPartitions = jobInfo->numPartitions;
    delete jobInfo;

    partitionMap.insert(std::pair<uint64_t, uint64_t>(jobID, numPartitions));

    return numPartitions;
  }
}

uint64_t PartitionMap::getNumPartitionsPerOutputDisk(uint64_t jobID) {
  return getNumPartitions(jobID) / totalNumOutputDisks;
}

uint64_t PartitionMap::getNumPartitionsPerNode(uint64_t jobID) {
  return getNumPartitions(jobID) / numNodes;
}

uint64_t PartitionMap::getNumPartitionsPerGroup(uint64_t jobID) {
  return getNumPartitions(jobID) / numPartitionGroups;
}

uint64_t PartitionMap::getFirstLocalPartition(uint64_t jobID) {
  return getNumPartitionsPerNode(jobID) * localNodeID;
}
