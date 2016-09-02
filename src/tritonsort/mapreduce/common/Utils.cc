#include <boost/math/common_factor.hpp>
#include <cmath>

#include "common/AlignmentUtils.h"
#include "core/Params.h"
#include "core/WorkerTracker.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/common/Utils.h"

void loadReadRequests(
  WorkerTracker& readerTracker, const Params& params,
  const std::string& phaseName) {
  uint64_t numDisks = params.get<uint64_t>("NUM_INPUT_DISKS");
  for (uint64_t diskID = 0; diskID < numDisks; diskID++) {
    CoordinatorClientInterface* coordinatorClient =
      CoordinatorClientFactory::newCoordinatorClient(
        params, phaseName, "reader", diskID);
    ReadRequest* readRequest = NULL;
    do {
      readRequest = coordinatorClient->getNextReadRequest();
      // No matter whether this is NULL or not, add it to the tracker.
      readerTracker.addWorkUnit(readRequest);
      // But if the request was NULL, then we should stop after adding it.
    } while (readRequest != NULL);

    delete coordinatorClient;
  }
}

uint64_t setNumPartitions(
  uint64_t jobID, double intermediateToInputRatio, const Params& params) {
  // Get the input data size for the job.
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(params, "", "", 0);
  JobInfo* jobInfo = coordinatorClient->getJobInfo(jobID);
  uint64_t totalInputSize = jobInfo->totalInputSize;
  delete jobInfo;

  // Compute intermediate data size
  uint64_t estimatedIntermediateDataSize =
    totalInputSize * intermediateToInputRatio;

  // Compute the number of partitions on each node and each disk
  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");
  uint64_t numIntermediateDisks =
    params.get<uint64_t>("NUM_OUTPUT_DISKS.phase_one");
  uint64_t numOutputDisks =
    params.get<uint64_t>("NUM_OUTPUT_DISKS.phase_two");
  uint64_t partitionSize = params.get<uint64_t>("PARTITION_SIZE");
  uint64_t partitionGroupsPerNode =
    params.get<uint64_t>("PARTITION_GROUPS_PER_NODE");

  uint64_t totalNumDisks = numNodes * numIntermediateDisks;
  uint64_t dataPerDisk = estimatedIntermediateDataSize / totalNumDisks;
  uint64_t partitionsPerDisk = std::max<uint64_t>(
      std::ceil(dataPerDisk / ((double) partitionSize)), 1);
  uint64_t partitionsPerNode = partitionsPerDisk * numIntermediateDisks;

  // Number of partitions per node must be a multiple of both the number of
  // disks and the number of partition groups per node, so round up.

  uint64_t partitionMultiple =
    boost::math::lcm(numIntermediateDisks, partitionGroupsPerNode);
  partitionMultiple = boost::math::lcm(numOutputDisks, partitionMultiple);
  partitionsPerNode = roundUp(partitionsPerNode, partitionMultiple);
  TRITONSORT_ASSERT(partitionsPerNode % numIntermediateDisks == 0,
         "Partitions per node %llu should be divisible by the number of "
         "intermediate disks %llu", partitionsPerNode, numIntermediateDisks);
  TRITONSORT_ASSERT(partitionsPerNode % numOutputDisks == 0,
         "Partitions per node %llu should be divisible by the number of "
         "output disks %llu", partitionsPerNode, numOutputDisks);
  TRITONSORT_ASSERT(partitionsPerNode % partitionGroupsPerNode == 0,
         "Partitions per node %llu should be divisible by the number of "
         "partition groups %llu", partitionsPerNode, partitionGroupsPerNode);

  // Finally compute the number of partitions for the job, and inform the
  // coordinator
  uint64_t numPartitions = partitionsPerNode * numNodes;
  coordinatorClient->setNumPartitions(jobID, numPartitions);

  delete coordinatorClient;

  return numPartitions;
}
