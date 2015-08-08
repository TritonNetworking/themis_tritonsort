#include "core/ScopedLock.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/RecoveryInfo.h"
#include "mapreduce/common/boundary/DiskBackedBoundaryKeyList.h"
#include "mapreduce/common/filter/RecordFilter.h"
#include "mapreduce/common/filter/RecordFilterMap.h"

RecordFilterMap::RecordFilterMap(const Params& _params)
  : params(_params) {
  pthread_mutex_init(&lock, NULL);

  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, "DUMMY_PHASE", "record_filter_map", 0);
}

RecordFilterMap::RecordFilterMap(
  const Params& _params, CoordinatorClientInterface* _coordinatorClient)
  : params(_params),
    coordinatorClient(_coordinatorClient) {
  pthread_mutex_init(&lock, NULL);
}

RecordFilterMap::RecordFilterMap::~RecordFilterMap() {
  pthread_mutex_lock(&lock);

  if (coordinatorClient != NULL) {
    delete coordinatorClient;
    coordinatorClient = NULL;
  }

  for (JobToRecordFilterMap::iterator iter = filterMap.begin();
       iter != filterMap.end(); iter++) {
    if (iter->second != NULL) {
      delete iter->second;
    }
  }
  filterMap.clear();

  pthread_mutex_unlock(&lock);

  pthread_mutex_destroy(&lock);
}

const RecordFilter* RecordFilterMap::get(uint64_t jobID) {
  typedef RecoveryInfo::PartitionRangeList PRL;

  ScopedLock scopedLock(&lock);

  JobToRecordFilterMap::iterator iter = filterMap.find(jobID);

  RecordFilter* recordFilter = NULL;

  if (iter != filterMap.end()) {
    recordFilter = iter->second;
  } else {
    // Figure out which partitions you're supposed to filter
    RecoveryInfo* recoveryInfo = coordinatorClient->getRecoveryInfo(jobID);

    if (recoveryInfo != NULL) {
      // If recoveryInfo is NULL, this job isn't recovering anything and we can
      // pass all records through the filter

      uint64_t recoveringJobID = recoveryInfo->recoveringJob;

      // Load appropriate boundary keys from disk
      DiskBackedBoundaryKeyList* boundaryList =
        DiskBackedBoundaryKeyList::loadForJob(params, recoveringJobID);

      const PRL& ranges = recoveryInfo->partitionRangesToRecover;

      recordFilter = new RecordFilter();

      // Construct record filter from those boundary keys
      for (PRL::const_iterator iter = ranges.begin(); iter != ranges.end();
           iter++) {
        uint64_t startPartition = iter->first;
        uint64_t endPartition = iter->second;

        PartitionBoundaries* boundaries = NULL;

        if (startPartition == endPartition) {
          boundaries = boundaryList->getPartitionBoundaries(startPartition);
        } else {
          boundaries = boundaryList->getPartitionBoundaries(
            startPartition, endPartition);
        }

        recordFilter->addPartitionBoundaries(*boundaries);
      }

      delete boundaryList;
      delete recoveryInfo;
    }

    filterMap.insert(std::make_pair(jobID, recordFilter));
  }

  return recordFilter;
}
