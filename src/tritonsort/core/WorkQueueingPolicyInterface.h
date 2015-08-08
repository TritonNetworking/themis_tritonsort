#ifndef THEMIS_WORK_QUEUEING_POLICY_INTERFACE_H
#define THEMIS_WORK_QUEUEING_POLICY_INTERFACE_H

#include "core/WorkQueue.h"

class Resource;

/**
   WorkQueueingPolicyInterface exposes a queue-like interface to worker
   trackers. Work units can be abstractly enqueued using whatever policy
   a particular stage requires. Work units can be dequeued in 3 ways:
   single work unit blocking dequeue, single work unit non-blocking dequeue,
   or batch dequeue (can return no work).
 */
class WorkQueueingPolicyInterface {
public:
  /// Destructor
  virtual ~WorkQueueingPolicyInterface() {}

  /**
     Enqueue a work unit

     \param workUnit the work unit to enqueue
   */
  virtual void enqueue(Resource* workUnit) = 0;

  /**
     Dequeue a work unit in blocking fashion from a particular queue.

     \param queueID the queue to dequeue from.
   */
  virtual Resource* dequeue(uint64_t queueID) = 0;

  /**
     Dequeue a work unit in non-blocking fashion from a particular queue.

     \param queueID the queue to dequeue from.

     \param[out] workUnit the dequeued work unit.

     \return true if dequeued a work unit, false otherwise
   */
  virtual bool nonBlockingDequeue(uint64_t queueID, Resource*& workUnit) = 0;

  /**
     Dequeue all work units from a particular queue. Will not block but may not
     dequeue any work units if none are present.

     \param queueID the queue to dequeue from.

     \param destinationQueue a destination work queue to store dequeued work
     units
   */
  virtual void batchDequeue(uint64_t queueID, WorkQueue& destinationQueue) = 0;

  /**
     Instruct the policy to teardown, which means no more work units will be
     received.
   */
  virtual void teardown() = 0;
};

#endif // THEMIS_WORK_QUEUEING_POLICY_INTERFACE_H
