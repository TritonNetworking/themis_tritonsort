#include "core/TritonSortAssert.h"
#include "mapreduce/common/ChunkMap.h"
#include "mapreduce/common/queueing/ByteStreamWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/ChunkingWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/FairDiskWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/MapReduceWorkQueueingPolicyFactory.h"
#include "mapreduce/common/queueing/MergerWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/NetworkDestinationWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/PartitionGroupWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/PhysicalDiskWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/ReadRequestWorkQueueingPolicy.h"

MapReduceWorkQueueingPolicyFactory::MapReduceWorkQueueingPolicyFactory()
  : chunkMap(NULL) {
}

WorkQueueingPolicyInterface*
MapReduceWorkQueueingPolicyFactory::newWorkQueueingPolicy(
  const std::string& phaseName, const std::string& stageName,
  const Params& params) const {

  std::string policyParamName(
    "WORK_QUEUEING_POLICY." + phaseName + "." + stageName);

  if (params.contains(policyParamName)) {
    const std::string& policyName = params.get<std::string>(policyParamName);
    uint64_t numWorkers = params.getv<uint64_t>(
      "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

    if (policyName == "ByteStreamWorkQueueingPolicy") {
      return new ByteStreamWorkQueueingPolicy(numWorkers);
    } else if (policyName == "FairDiskWorkQueueingPolicy") {
      uint64_t numDisks = params.getv<uint64_t>(
        "NUM_OUTPUT_DISKS.%s", phaseName.c_str());
      return new FairDiskWorkQueueingPolicy(numDisks, params, phaseName);
    } else if (policyName == "NetworkDestinationWorkQueueingPolicy") {
      uint64_t partitionGroupsPerNode =
        params.get<uint64_t>("PARTITION_GROUPS_PER_NODE");
      uint64_t numPeers = params.get<uint64_t>("NUM_PEERS");
      return new NetworkDestinationWorkQueueingPolicy(
        partitionGroupsPerNode, numPeers);
    } else if (policyName == "PartitionGroupWorkQueueingPolicy") {
      uint64_t partitionGroupsPerNode =
        params.get<uint64_t>("PARTITION_GROUPS_PER_NODE");
      return new PartitionGroupWorkQueueingPolicy(
        partitionGroupsPerNode, numWorkers);
    } else if (policyName == "PhysicalDiskWorkQueueingPolicy") {
      uint64_t disksPerWorker = params.getv<uint64_t>(
        "DISKS_PER_WORKER.%s.%s", phaseName.c_str(), stageName.c_str());
      return new PhysicalDiskWorkQueueingPolicy(
        disksPerWorker, numWorkers, params, phaseName);
    } else if (policyName == "ReadRequestWorkQueueingPolicy") {
      return new ReadRequestWorkQueueingPolicy(numWorkers);
    } else if (policyName == "ChunkingWorkQueueingPolicy") {
      uint64_t disksPerWorker = params.getv<uint64_t>(
        "DISKS_PER_WORKER.%s.%s", phaseName.c_str(), stageName.c_str());
      ABORT_IF(chunkMap == NULL, "Must set chunk map");
      return new ChunkingWorkQueueingPolicy(
        disksPerWorker, numWorkers, *chunkMap);
    } else if (policyName == "MergerWorkQueueingPolicy") {
      ABORT_IF(chunkMap == NULL, "Must set chunk map");

      const ChunkMap::DiskMap& diskMap = chunkMap->getDiskMap();
      uint64_t totalNumChunks = 0;
      for (ChunkMap::DiskMap::const_iterator iter = diskMap.begin();
           iter != diskMap.end(); iter++) {
        totalNumChunks += (iter->second).size();
      }

      return new MergerWorkQueueingPolicy(totalNumChunks, *chunkMap);
    } else {
      ABORT("Unknown queueing policy '%s'", policyName.c_str());
      return NULL;
    }
  } else {
    // No policy specified, let the default factory select a policy.
    return WorkQueueingPolicyFactory::newWorkQueueingPolicy(
      phaseName, stageName, params);
  }
}

void MapReduceWorkQueueingPolicyFactory::setChunkMap(ChunkMap* _chunkMap) {
  chunkMap = _chunkMap;
}
