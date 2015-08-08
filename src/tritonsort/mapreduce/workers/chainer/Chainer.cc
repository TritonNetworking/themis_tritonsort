#include "common/WriteTokenPool.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/buffers/ListableKVPairBuffer.h"
#include "mapreduce/workers/chainer/Chainer.h"

Chainer::Chainer(
  uint64_t id, const std::string& name, uint64_t _nodeID,
  uint64_t _physicalDisksPerChainer, uint64_t maxBytesInBufferTable,
  uint64_t _workUnitEmissionLowerBound, uint64_t _workUnitEmissionUpperBound,
  uint64_t _numCoalescers, uint64_t _numNodes, uint64_t _numDisks,
  WriteTokenPool& _writeTokenPool, const Params& params,
  const std::string& phaseName)
  : BatchRunnable(id, name, maxBytesInBufferTable),
    nodeID(_nodeID),
    physicalDisksPerChainer(_physicalDisksPerChainer),
    workUnitEmissionLowerBound(_workUnitEmissionLowerBound),
    workUnitEmissionUpperBound(_workUnitEmissionUpperBound),
    numCoalescers(_numCoalescers),
    numNodes(_numNodes),
    numDisks(_numDisks),
    writeTokenPool(_writeTokenPool),
    partitionMap(params, phaseName),
    logger(name, id) {

  failedGetAttempts = 0;
  iterationStartRateLimitingCounter = 0;
  workUnitEmittedRateLimitingCounter = 0;
  numAttemptGets = 0;
  numSuccessfulGets = 0;
  emptyWorkQueueWaits = 0;

  pthread_rwlock_init(&bufferTablesLock, NULL);

  runTimestampStatID = logger.registerStat("run_timestamp");
  emitWorkTimestampStatID = logger.registerStat("emit_work_timestamp");
}

Chainer::~Chainer() {
  pthread_rwlock_destroy(&bufferTablesLock);
}

void Chainer::resourceMonitorOutput(Json::Value& obj) {
  BatchRunnable::resourceMonitorOutput(obj);

  pthread_rwlock_rdlock(&bufferTablesLock);
  for (BufferTableMap::const_iterator iter = bufferTables.begin();
       iter != bufferTables.end(); iter++) {
    const BufferTable<ListableKVPairBuffer>* bufferTable = iter->second;

    obj["total_buffers_in_table"] = Json::UInt64(
      bufferTable->getTotalNumBuffersInTable());

    BufferList<ListableKVPairBuffer>* largestList =
      bufferTable->getLargestList();

    if (largestList != NULL) {
      obj["largest_list_bytes"] = Json::UInt64(
        largestList->getTotalDataSize());
    } else {
      obj["largest_list_bytes"] = 0;
    }
  }
  pthread_rwlock_unlock(&bufferTablesLock);
}

void Chainer::teardown() {
  std::set<uint64_t> physicalDisksWithWork;

  pthread_rwlock_wrlock(&bufferTablesLock);
  for (BufferTableMap::iterator iter = bufferTables.begin();
       iter != bufferTables.end(); iter++) {
    uint64_t jobID = iter->first;
    BufferTable<ListableKVPairBuffer>* bufferTable = iter->second;

    bufferTable->getPhysicalDisksWithListsAboveMinimumSize(
      0, physicalDisksWithWork);

    uint64_t numDisksWithWork = physicalDisksWithWork.size();

    while (numDisksWithWork > 0) {
      WriteToken* token = writeTokenPool.getToken(physicalDisksWithWork);

      LBLContainer* lblContainer = new (themis::memcheck) LBLContainer(
        jobID, token);

      uint64_t diskID = lblContainer->getDiskID();

      BufferList<ListableKVPairBuffer>* list =
        bufferTable->getLargestListForDisk(diskID);

      if (list == NULL) {
        physicalDisksWithWork.erase(diskID);
        numDisksWithWork--;
        delete lblContainer;

        if (token != NULL) {
          writeTokenPool.putToken(token);
        }
        continue;
      }
      ASSERT(list != NULL);

      writeListToBuffer(bufferTable, list, lblContainer);
      emitWorkUnit(lblContainer);
    }

    delete bufferTable;
  }
  bufferTables.clear();
  pthread_rwlock_unlock(&bufferTablesLock);

  logger.logDatum("failed_get_attempts", failedGetAttempts);
  logger.logDatum("num_attempt_gets", numAttemptGets);
  logger.logDatum("num_successful_gets", numSuccessfulGets);
  logger.logDatum("empty_work_queue_waits", emptyWorkQueueWaits);
}

void Chainer::run(WorkQueue& workQueue) {
  while (true) {
    // If there's no more work to append, we're done with normal behavior and
    // it's time to start tearing down.
    if (workQueue.empty() && workQueue.willNotReceiveMoreWork()) {
      break;
    }

    this->getNewWork();

    // If you don't get new work, wait 1ms for work to come in.
    if (workQueue.empty()) {
      emptyWorkQueueWaits++;
      this->startWaitForWorkTimer();
      usleep(1000);
      this->stopWaitForWorkTimer();
    } else {
      while (!workQueue.empty()) {
        ListableKVPairBuffer* workUnit =
          dynamic_cast<ListableKVPairBuffer*>(workQueue.front());

        ABORT_IF(workUnit == NULL,
                 "Got NULL or incorrectly typed work unit.");
        workQueue.pop();

        const std::set<uint64_t>& jobIDs = workUnit->getJobIDs();
        ASSERT(jobIDs.size() == 1, "Expected this buffer to have exactly one "
               "job ID");

        uint64_t currentJobID = *(jobIDs.begin());

        BufferTable<ListableKVPairBuffer>* bufferTable = NULL;

        // Need to grab the buffer table for the current job. In the common
        // case, we'll only have to do a lookup into the table, so start with a
        // read lock
        pthread_rwlock_rdlock(&bufferTablesLock);

        BufferTableMap::iterator tableIter = bufferTables.find(currentJobID);

        if (tableIter != bufferTables.end()) {
          bufferTable = tableIter->second;
        } else {
          // We haven't encountered any buffers for this buffer table before,
          // so we need to create it

          uint64_t partitionsPerDisk =
            partitionMap.getNumPartitionsPerOutputDisk(currentJobID);

          bufferTable = new (themis::memcheck)
            BufferTable<ListableKVPairBuffer>(
              id * physicalDisksPerChainer, physicalDisksPerChainer, nodeID,
              partitionsPerDisk, numDisks);

          // Since we have to insert this new buffer table into the map,
          // release read lock on the table and re-acquire a write lock. We
          // know that we can safely mutate the table in this way since the
          // only other entity that's fiddling with this lock is the resource
          // monitor, and it's not modifying anything; otherwise this could
          // race

          pthread_rwlock_unlock(&bufferTablesLock);
          pthread_rwlock_wrlock(&bufferTablesLock);
          bufferTables.insert(std::make_pair(currentJobID, bufferTable));
        }

        pthread_rwlock_unlock(&bufferTablesLock);

        ASSERT(bufferTable != NULL, "Should have assigned buffer table at this "
               "point");

        bufferTable->insert(workUnit);
        internalStateSize += workUnit->getCurrentSize();
      }
    }

    pthread_rwlock_rdlock(&bufferTablesLock);
    for (BufferTableMap::iterator iter = bufferTables.begin();
         iter != bufferTables.end(); iter++) {
      writeFullLists(iter->first, iter->second);
    }
    pthread_rwlock_unlock(&bufferTablesLock);
  }
}

Chainer::LBLContainer* Chainer::tryToGetListContainer(
  uint64_t jobID, const std::set<uint64_t>& physicalDisksWithWork) {

  WriteToken* token = writeTokenPool.attemptGetToken(physicalDisksWithWork);

  if (token != NULL) {
    LBLContainer* lblContainer = new LBLContainer(jobID, token);
    return lblContainer;
  } else {
    failedGetAttempts++;
    return NULL;
  }
}

void Chainer::writeListToBuffer(
  BufferTable<ListableKVPairBuffer>* bufferTable,
  BufferList<ListableKVPairBuffer>* inputList, LBLContainer* container) {

  BufferList<ListableKVPairBuffer>& outputList = container->getList();
  outputList.clear();
  outputList.setLogicalDiskID(inputList->getLogicalDiskID());
  outputList.setPhysicalDiskID(inputList->getPhysicalDiskID());

  if (inputList->getSize() > 0 &&
      (inputList->getHead()->getCurrentSize() > workUnitEmissionUpperBound)) {
    // If the list's head is so large that it exceeds the work unit emission
    // upper bound, append it to the output list and move on. Otherwise,
    // you'd never emit this list because its first buffer is too large.

    ListableKVPairBuffer* head = inputList->removeHead();
    outputList.append(head);
  } else {
    inputList->bulkMoveBuffersTo(outputList, workUnitEmissionUpperBound);
  }

  bufferTable->updateLargestList(inputList->getPhysicalDiskID());
}

void Chainer::writeFullLists(
  uint64_t jobID, BufferTable<ListableKVPairBuffer>* bufferTable) {

  while (true) {
    physicalDisksWithFullLists.clear();

    bufferTable->getPhysicalDisksWithListsAboveMinimumSize(
      workUnitEmissionLowerBound, physicalDisksWithFullLists);

    if (physicalDisksWithFullLists.size() == 0) {
      break;
    }

    LBLContainer* lblContainer = tryToGetListContainer(
      jobID, physicalDisksWithFullLists);
    numAttemptGets++;

    if (lblContainer != NULL) {
      numSuccessfulGets++;
      uint64_t diskID = lblContainer->getDiskID();

      BufferList<ListableKVPairBuffer>* list =
        bufferTable->getLargestListForDisk(diskID);
      ASSERT(list != NULL);
      writeListToBuffer(bufferTable, list, lblContainer);
      emitWorkUnit(lblContainer);
    } else {
      break;
    }
  }
}

BaseWorker* Chainer::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t nodeID = params.get<uint64_t>("MYPEERID");
  uint64_t physicalDisksPerChainer = params.getv<uint64_t>(
    "DISKS_PER_WORKER.%s.%s", phaseName.c_str(), stageName.c_str());
  uint64_t workUnitEmissionLowerBound = params.get<uint64_t>(
    "CHAINER_WORK_UNIT_EMISSION_LOWER_BOUND");
  uint64_t workUnitEmissionUpperBound = params.get<uint64_t>(
    "CHAINER_WORK_UNIT_EMISSION_UPPER_BOUND");
  uint64_t maxBytesInBufferTable = params.get<uint64_t>(
    "MAX_CHAINER_BUFFER_TABLE_SIZE");
  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");
  uint64_t numDisks =
    params.getv<uint64_t>("NUM_OUTPUT_DISKS.%s", phaseName.c_str());

  std::string* coalescerStageName = dependencies.get<std::string>(
    stageName, "coalescer_stage_name");

  uint64_t numCoalescers = params.get<uint64_t>(
    "NUM_WORKERS." + phaseName + "." + *coalescerStageName);

  WriteTokenPool* writeTokenPool = dependencies.get<WriteTokenPool>(
    "write_token_pool");

  Chainer* chainer = new Chainer(
    id, stageName, nodeID, physicalDisksPerChainer,
    maxBytesInBufferTable, workUnitEmissionLowerBound,
    workUnitEmissionUpperBound, numCoalescers, numNodes, numDisks,
    *writeTokenPool, params, phaseName);

  return chainer;
}
