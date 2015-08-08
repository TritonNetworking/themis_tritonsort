#include <sstream>

#include "core/Params.h"
#include "core/ScopedLock.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/boundary/KeyPartitioner.h"
#include "mapreduce/functions/partition/PartitionFunctionFactory.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"

PartitionFunctionMap::PartitionFunctionMap(
  const Params& _params, const std::string& phaseName)
  : params(_params),
    coordinatorClient(NULL),
    partitionMap(_params, phaseName) {
  pthread_mutex_init(&lock, NULL);

  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, "DUMMY_PHASE", "partition_function_map", 0);
}

PartitionFunctionMap::~PartitionFunctionMap() {
  pthread_mutex_lock(&lock);

  if (coordinatorClient != NULL) {
    delete coordinatorClient;
    coordinatorClient = NULL;
  }

  for (InternalFunctionMap::iterator iter = functions.begin();
       iter != functions.end(); iter++) {
    delete iter->second;
  }

  functions.clear();

  for (std::list<KeyPartitionerInterface*>::iterator iter =
         keyPartitioners.begin();
       iter != keyPartitioners.end(); iter++) {
    delete *iter;
  }

  keyPartitioners.clear();

  pthread_mutex_unlock(&lock);

  pthread_mutex_destroy(&lock);
}

PartitionFunctionInterface* const PartitionFunctionMap::get(uint64_t jobID) {
  ScopedLock scopedLock(&lock);

  InternalFunctionMap::iterator iter = functions.find(jobID);

  if (iter != functions.end()) {
    return iter->second;
  } else {

    KeyPartitionerInterface* keyPartitioner = NULL;

    std::ostringstream oss;
    oss << "BOUNDARY_LIST_FILE." << jobID;

    std::string keyPartitionerFileParam(oss.str());
    if (params.contains(keyPartitionerFileParam)) {
      const std::string& keyPartitionerFilename = params.get<std::string>(
        keyPartitionerFileParam);

      if (fileExists(keyPartitionerFilename)) {
        File keyPartitionerFile(keyPartitionerFilename);
        keyPartitioner = new KeyPartitioner(keyPartitionerFile);
        keyPartitioners.push_back(keyPartitioner);

        // Check that the number of partition groups matches the number of
        // global partitions.
        uint64_t numPartitionGroups =
          params.get<uint64_t>("NUM_PARTITION_GROUPS");
        ABORT_IF(keyPartitioner->numGlobalPartitions() != numPartitionGroups,
                 "KeyPartitioner loaded from %s contains %llu global "
                 "partitions, but there should be %llu.",
                 keyPartitionerFileParam.c_str(),
                 keyPartitioner->numGlobalPartitions(), numPartitionGroups);
      }
    }

    // Create a factory using the newly loaded key partitioner.
    PartitionFunctionFactory factory(
      keyPartitioner, partitionMap.getNumPartitions(jobID));

    // Create a new partition function from the factory.
    JobInfo* jobInfo = coordinatorClient->getJobInfo(jobID);
    PartitionFunctionInterface* partitionFunction =
      factory.newPartitionFunctionInstance(params, jobInfo->partitionFunction);
    delete jobInfo;

    // Update the map.
    functions.insert(
      std::pair<uint64_t, PartitionFunctionInterface*>(
        jobID, partitionFunction));

    return partitionFunction;
  }
}

