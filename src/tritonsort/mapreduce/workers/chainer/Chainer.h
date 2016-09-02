#ifndef TRITONSORT_CHAINER_H
#define TRITONSORT_CHAINER_H

#include <pthread.h>

#include "mapreduce/common/PartitionMap.h"
#include "common/BufferListContainer.h"
#include "common/buffers/BufferTable.h"
#include "core/BatchRunnable.h"

class ListableKVPairBuffer;
class WriteTokenPool;

/**
   The chainer is responsible for chaining together logical disk buffers into a
   chain per logical disk, and passing chains to the next stage when they're
   long enough.
 */
class Chainer : public BatchRunnable {
  WORKER_IMPL

public:
  /// Shorthand for a BufferListContainer containing the appropriate logical
  /// disk buffer type
  typedef BufferListContainer<ListableKVPairBuffer> LBLContainer;

  /// Constructor
  /**
     \param id the worker's ID

     \param name the name of the stage for which the worker is working

     \param nodeID the node ID of the node that stores this BufferTable

     \param physicalDisksPerChainer the number of physical disks assigned to
     each Chainer worker in this phase

     \param maxBytesInBufferTable the maximum number of bytes that are allowed
     to be resident in this worker's buffer table at any one time

     \param workUnitEmissionLowerBound the smallest amount of data, in bytes,
     that a given logical disk buffer chain can hold before it is eligible for
     emission to the next stage

     \param workUnitEmissionUpperBound the largest amount of data, in bytes,
     contained in a logical disk buffer chain emitted to the next stage

     \param numCoalescers the number of downstream coalescers to which the
     chainer is routing chains. All chains for a given logical disk must be
     routed to the same coalescer so that buffer alignment is done correctly

     \param numNodes the number of nodes in the cluster

     \param numDisks the number of intermediate disks in the cluster

     \param writeTokenPool a pool of write tokens that the chainer will use to
     self-regulate its emission of work to the writer stage

     \param params the global params object

     \param phaseName the name of the phase
   */
  Chainer(
    uint64_t id, const std::string& name, uint64_t nodeID,
    uint64_t physicalDisksPerChainer, uint64_t maxBytesInBufferTable,
    uint64_t workUnitEmissionLowerBound, uint64_t workUnitEmissionUpperBound,
    uint64_t numCoalescers, uint64_t numNodes, uint64_t numDisks,
    WriteTokenPool& writeTokenPool, const Params& params,
    const std::string& phaseName);

  virtual ~Chainer();

  void resourceMonitorOutput(Json::Value& obj);

  inline void emitWorkUnit(LBLContainer* container) {
    workUnitEmittedRateLimitingCounter++;
    if (workUnitEmittedRateLimitingCounter % 100 == 0) {
      logger.add(emitWorkTimestampStatID, Timer::posixTimeInMicros());
    }

    TRITONSORT_ASSERT(container->getList().getTotalDataSize() > 0,
           "Chainer should not emit empty lists.");

    internalStateSize -= container->getCurrentSize();

    BaseWorker::emitWorkUnit(container);
  }

  /// Perform any final actions after work unit processing has ceased but
  /// before destruction
  /**
     Emits any remaining logical disk buffer chains as buffers become available
     for them from the writer.
   */
  void teardown();

  /**
     Until we run out of work, continuously do the following:
     - Get as many logical disk buffers as possible from the work queue
        - If the work queue is empty, wait 1ms and try again
        - Otherwise, append all received logical disk buffers to the appropriate
          list
     - Append any lists that are full enough (see Chainer::writeFullLists)

     \sa BatchRunnable::run
   */
  void run(WorkQueue& workQueue);

private:
  typedef std::map< uint64_t, BufferTable<ListableKVPairBuffer>* >
  BufferTableMap;

  LBLContainer* tryToGetListContainer(
    uint64_t jobID, const std::set<uint64_t>& physicalDisksWithWork);

  void writeListToBuffer(
    BufferTable<ListableKVPairBuffer>* bufferTable,
    BufferList<ListableKVPairBuffer>* inputList, LBLContainer* container);

  void writeFullLists(
    uint64_t jobID, BufferTable<ListableKVPairBuffer>* bufferTable);

  const uint64_t nodeID;
  const uint64_t physicalDisksPerChainer;
  const uint64_t workUnitEmissionLowerBound;
  const uint64_t workUnitEmissionUpperBound;
  const uint64_t numCoalescers;

  const uint64_t numNodes;
  const uint64_t numDisks;

  WriteTokenPool& writeTokenPool;
  PartitionMap partitionMap;

  pthread_rwlock_t bufferTablesLock;
  BufferTableMap bufferTables;

  std::set<uint64_t> physicalDisksWithFullLists;

  uint64_t failedGetAttempts;
  uint64_t iterationStartRateLimitingCounter;
  uint64_t workUnitEmittedRateLimitingCounter;
  uint64_t numAttemptGets;
  uint64_t numSuccessfulGets;
  uint64_t emptyWorkQueueWaits;

  uint64_t runTimestampStatID;
  uint64_t emitWorkTimestampStatID;

  StatLogger logger;
};

#endif // TRITONSORT_CHAINER_H
