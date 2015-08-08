#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/queueing/FairDiskWorkQueueingPolicy.h"
#include "mapreduce/common/queueing/PhysicalDiskWorkQueueingPolicy.h"

FairDiskWorkQueueingPolicy::FairDiskWorkQueueingPolicy(
  uint64_t _numDisks, const Params& params, const std::string& phaseName)
  : numDisks(_numDisks),
    partitionMap(params, phaseName),
    nextQueueID(0),
    numWorkUnits(0),
    done(false) {
  workQueues.resize(numDisks);

  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&waitingForEnqueue, NULL);
}

FairDiskWorkQueueingPolicy::~FairDiskWorkQueueingPolicy() {
  // Sanity check that all queues are empty.
  uint64_t queueID = 0;
  for (WorkQueueVector::iterator iter = workQueues.begin();
       iter != workQueues.end(); iter++, queueID++) {
    ASSERT(iter->empty(), "At tracker destruction time, queue %llu still has "
           "%llu work units.", queueID, iter->size());
  }

  // Sanity check that we've call teardown()
  ASSERT(done,
         "At destruction time teardown() must have been previously called");

  pthread_mutex_destroy(&lock);
  pthread_cond_destroy(&waitingForEnqueue);
}

void FairDiskWorkQueueingPolicy::enqueue(Resource* workUnit) {
  ScopedLock scopedLock(&lock);
  ASSERT(workUnit != NULL, "Cannot enqueue NULL work unit. If you're trying to "
         "tear down a queue, call teardown() instead");
  ASSERT(!done, "Cannot enqueue more work units after teardown.");

  // Use the PhysicalDiskWorkQueueingPolicy to determine the queue ID.
  uint64_t queueID = PhysicalDiskWorkQueueingPolicy::computeDisk(
    workUnit, partitionMap, numDisks);

  workQueues[queueID].push(workUnit);
  numWorkUnits++;

  // If there are threads blocked on dequeue, signal one of them.
  pthread_cond_signal(&waitingForEnqueue);
}

Resource* FairDiskWorkQueueingPolicy::dequeue(uint64_t requestedQueueID) {
  ScopedLock scopedLock(&lock);
  while (numWorkUnits == 0 && !done) {
    // We need to block until a thread enqueues more work.
    pthread_cond_wait(&waitingForEnqueue, &lock);
  }

  // If we're done, we'll return NULL, but otherwise pick the next
  // round-robin work queue.
  Resource* resource = NULL;
  if (numWorkUnits > 0) {
    resource = getNextRoundRobinWorkUnit();
  }

  return resource;
}

bool FairDiskWorkQueueingPolicy::nonBlockingDequeue(
  uint64_t requestedQueueID, Resource*& workUnit) {
  ScopedLock scopedLock(&lock);

  if (numWorkUnits == 0) {
    // There's no work in the queue, so return false, unless we're
    // done, in which case return true with a NULL work unit to signal
    // end-of-queue
    workUnit = NULL;
    return done;
  }

  // We are guaranteed to have at least one non-empty queue. Ignore the
  // requested queueID and get pick the next round robin queue.
  workUnit = getNextRoundRobinWorkUnit();

  return true;
}

void FairDiskWorkQueueingPolicy::batchDequeue(
  uint64_t queueID, WorkQueue& destinationQueue) {
  ScopedLock scopedLock(&lock);
  // We can't just splice queues like in the case of the regular
  // WorkQueueingPolicy, since we have to emit in round-robin order, so simply
  // fetch work units until there are none left.
  while (numWorkUnits > 0) {
    Resource* workUnit = getNextRoundRobinWorkUnit();
    destinationQueue.push(workUnit);
  }

  // If we're done, also enqueue a NULL work until to signal end-of-queue.
  if (done) {
    destinationQueue.push(NULL);
  }
}

void FairDiskWorkQueueingPolicy::teardown() {
  ScopedLock scopedLock(&lock);
  done = true;
  pthread_cond_broadcast(&waitingForEnqueue);
}

Resource* FairDiskWorkQueueingPolicy::getNextRoundRobinWorkUnit() {
  ASSERT(numWorkUnits > 0, "Must have at least one work unit.");

  // Find the next non-empty round-robin queue.
  uint64_t attempt = 0;
  for (attempt = 0;
       workQueues[(nextQueueID + attempt) % numDisks].empty() &&
         attempt < numDisks;
       attempt++) {
  }

  ASSERT(attempt < numDisks,
         "Could not find a non-empty queue (%llu disks).", numDisks);

  uint64_t queueID = (nextQueueID + attempt) % numDisks;
  nextQueueID = (queueID + 1) % numDisks;

  // Now pop and return the work unit.
  WorkQueue& queue = workQueues[queueID];
  Resource* workUnit = queue.front();
  queue.pop();
  numWorkUnits--;

  return workUnit;
}
