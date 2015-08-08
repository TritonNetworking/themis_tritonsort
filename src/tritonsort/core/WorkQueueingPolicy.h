#ifndef THEMIS_WORK_QUEUEING_POLICY_H
#define THEMIS_WORK_QUEUEING_POLICY_H

#include "core/ThreadSafeVector.h"
#include "core/ThreadSafeWorkQueue.h"
#include "core/WorkQueueingPolicyInterface.h"
#include "core/constants.h"

/**
   WorkQueueingPolicy implements WorkQueueingPolicyInterface with a
   ThreadSafeVector of ThreadSafeWorkQueues.  It is therefore possible for
   multiple threads to enqueue and dequeue work units without explicit
   coordination.

   The actual choice of queue is abstracted in the two methods getEnqueueID()
   and getDequeueID(). By default, work units are enqueued into queue 0 and
   dequeued from the requested queueID mod the number of queues. You can
   override these methods to get any kind of queueing policy desired.

   The only limitation to this implementation is that the choice of queue
   happens independently of the actual push and pop operations, so if the choice
   of queue depends on the state of the queue (for example if you wanted to
   implement a round robin policy), then this implementation cannot be used. In
   such a case you'll have to derive from the WorkQueueingPolicyInterface
   itself.
 */
class WorkQueueingPolicy : public WorkQueueingPolicyInterface {
public:
  /// Constructor
  /**
     \param numQueues the number of queues this policy contains
   */
  WorkQueueingPolicy(uint64_t numQueues);

  /// Destructor
  ~WorkQueueingPolicy();

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

protected:
  const uint64_t numQueues;

private:
  typedef ThreadSafeVector<ThreadSafeWorkQueue> WorkQueueVector;

  /**
     Choose where a work unit should be enqueued. By default, place all work
     works in the first queue. Override this function to achieve the desired
     queueing policy.

     \param workUnit the work unit to enqueue

     \return the queue into which the work unit should be enqueued
   */
  virtual uint64_t getEnqueueID(Resource* workUnit);

  /**
     Choose where work units should be dequeued from. By default, coerce the
     requested queue ID into one of the available queues by taking it mod the
     number of queues. Override this function to achieve the desired queueing
     policy.

     \param queueID the queueID requested by the calling thread, usually a
     worker ID

     \return the queue from which the work unit should be dequeued
   */
  virtual uint64_t getDequeueID(uint64_t queueID);

  WorkQueueVector workQueues;

  DISALLOW_COPY_AND_ASSIGN(WorkQueueingPolicy);
};

#endif // THEMIS_WORK_QUEUEING_POLICY_H
