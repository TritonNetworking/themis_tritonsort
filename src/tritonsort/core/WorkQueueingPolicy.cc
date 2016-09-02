#include "core/TritonSortAssert.h"
#include "core/WorkQueueingPolicy.h"

WorkQueueingPolicy::WorkQueueingPolicy(uint64_t _numQueues)
  : numQueues(_numQueues) {
  workQueues.resize(numQueues);
}

WorkQueueingPolicy::~WorkQueueingPolicy() {
  // Sanity check that all queues are empty.
  workQueues.beginThreadSafeIterate();
  uint64_t queueID = 0;
  for (WorkQueueVector::iterator iter = workQueues.begin();
       iter != workQueues.end(); iter++, queueID++) {
    TRITONSORT_ASSERT(iter->empty(), "At tracker destruction time, queue %llu still has "
           "%llu work units.", queueID, iter->size());
  }
  workQueues.endThreadSafeIterate();
}

void WorkQueueingPolicy::enqueue(Resource* workUnit) {
  TRITONSORT_ASSERT(workUnit != NULL, "Cannot enqueue NULL work unit. If you're trying to "
         "tear down a queue, call teardown() instead");
  uint64_t queueID = getEnqueueID(workUnit);
  workQueues[queueID].push(workUnit);
}

Resource* WorkQueueingPolicy::dequeue(uint64_t requestedQueueID) {
  uint64_t queueID = getDequeueID(requestedQueueID);
  ThreadSafeWorkQueue& sourceWorkQueue = workQueues[queueID];
  Resource* resource = sourceWorkQueue.blockingPop();

  return resource;
}

bool WorkQueueingPolicy::nonBlockingDequeue(
  uint64_t requestedQueueID, Resource*& workUnit) {
  uint64_t queueID = getDequeueID(requestedQueueID);
  ThreadSafeWorkQueue& sourceWorkQueue = workQueues[queueID];
  bool noMoreWork = false;
  bool gotNewWork = sourceWorkQueue.pop(workUnit, noMoreWork);
  if (!gotNewWork && noMoreWork) {
    // This queue is empty and will never have more work, so return True with
    // a NULL work unit.
    workUnit = NULL;
    gotNewWork = true;
  }

  return gotNewWork;
}

void WorkQueueingPolicy::batchDequeue(
  uint64_t requestedQueueID, WorkQueue& destinationQueue) {
  uint64_t queueID = getDequeueID(requestedQueueID);
  ThreadSafeWorkQueue& sourceWorkQueue = workQueues[queueID];
  sourceWorkQueue.moveWorkToQueue(destinationQueue);
}

void WorkQueueingPolicy::teardown() {
  // Push NULL to all work queues so that workers know to shut down
  workQueues.beginThreadSafeIterate();
  for (WorkQueueVector::iterator iter = workQueues.begin();
       iter != workQueues.end(); ++iter) {
    iter->push(NULL);
  }
  workQueues.endThreadSafeIterate();
}

uint64_t WorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  // Use a single queue unless overwritten by a custom policy.
  return 0;
}

uint64_t WorkQueueingPolicy::getDequeueID(uint64_t queueID) {
  // Coerce the requested queue ID into one of our queues.
  return queueID % numQueues;
}
