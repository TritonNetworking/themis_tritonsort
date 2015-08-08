#ifndef MAPRED_FAIR_DISK_WORK_QUEUEING_POLICY_H
#define MAPRED_FAIR_DISK_WORK_QUEUEING_POLICY_H

#include <pthread.h>
#include <vector>

#include "core/WorkQueue.h"
#include "core/WorkQueueingPolicyInterface.h"
#include "core/constants.h"
#include "mapreduce/common/PartitionMap.h"

/**
   FairDiskWorkQueueingPolicy is a tracker queuing policy designed for stages
   that precede a writer stage but don't necessarily have to deal with data
   for specific disks. The goal of this policy is to cause data destined for
   disks to be processed in a fair, round-robin order to equalize load on
   each of the disks. A good use-case would be the reducer stage, which doesn't
   itself care about which disk it processes partitions for, but still should
   emit buffers equally to the writers.

   This queueing policy automatically inspects the logical disk ID of a
   worker buffer and assigns it the appropriate worker queue. However,
   the actual queue selected by downstream workers is determined based
   on a round-robin counter.
 */
class FairDiskWorkQueueingPolicy : public WorkQueueingPolicyInterface {
public:
  /// Constructor
  /**
     \param numDisks the number of physical disks

     \param params the global params object

     \param phaseName the name of the phase
   */
  FairDiskWorkQueueingPolicy(
    uint64_t numDisks, const Params& params, const std::string& phaseName);

  /// Destructor
  ~FairDiskWorkQueueingPolicy();

  /// \sa WorkQueueingPolicyInterface::enqueue
  void enqueue(Resource* workUnit);

  /// \sa WorkQueueingPolicyInterface::dequeue
  Resource* dequeue(uint64_t queueID);

  /// \sa WorkQueueingPolicyInterface::nonBlockingDequeue
  bool nonBlockingDequeue(uint64_t queueID, Resource*& workUnit);

  /// \sa WorkQueueingPolicyInterface::batchDequeue
  void batchDequeue(uint64_t queueID, WorkQueue& destinationQueue);

  /// \sa WorkQueueingPolicyInterface::teardown
  void teardown();

private:
  typedef std::vector<WorkQueue> WorkQueueVector;

  /**
     Pop a work unit from the first round-robin queue

     \return the next work unit
   */
  Resource* getNextRoundRobinWorkUnit();

  const uint64_t numDisks;
  PartitionMap partitionMap;

  uint64_t nextQueueID;
  uint64_t numWorkUnits;

  bool done;

  WorkQueueVector workQueues;

  pthread_mutex_t lock;
  pthread_cond_t waitingForEnqueue;

  DISALLOW_COPY_AND_ASSIGN(FairDiskWorkQueueingPolicy);
};

#endif // MAPRED_FAIR_DISK_WORK_QUEUEING_POLICY_H
