#ifndef TRITONSORT_MLFQ_POLICY_H
#define TRITONSORT_MLFQ_POLICY_H

#include <list>

#include "SchedulerPolicy.h"

/**
   A MLFQPolicy is a scheduler policy that implements a simple form of multi
   level feedback queue.  There are two queues: lowPriorityQueue and
   highPriorityQueue. Requests are added to the low priority queue, which is
   serviced by picking the earliest request that can be satisfied immediately.
   For example, if there are 500 resources available, and the low priority queue
   looks like:

   800 400 900 100 500

   then the 400 request will be scheduled because it is the closest request to
   the head that wants 500 or less resources.

   If a request sits in the low priority queue for a period of time, it gets
   escalated to the high priority queue, which is serviced first come first
   serve by acting as a FIFO queue. This timeout period is equal to the
   running average of the amount of time a resource was used by the application
   before being returned. This timeout period is arbitrary, and we are hoping to
   study the timeout period in more detail (\todo MC).

   This policy has the advantage that large requests do not block small requests
   from using resources more efficiently (due to low priority queue), and also
   guarantees that large requests do not starve (due to high priority queue).

   This policy relies on the recordUseTime() policy method, and so cannot be
   used if usage times cannot be tracked. For example, if resources are mutated
   before before returning, as in the case of bulkGet/bulkPut, then MLFQ cannot
   be used because usage times cannot be accurately tracked.
 */
class MLFQPolicy : public SchedulerPolicy {
public:
  /// Constructor
  MLFQPolicy();

  /// Destructor
  virtual ~MLFQPolicy();

  /**
     \sa SchedulerPolicy::addRequest
   */
  void addRequest(Request& request);

  /**
     \sa SchedulerPolicy::removeRequest
   */
  void removeRequest(Request& request, bool force=false);

  /**
     \sa SchedulerPolicy::canScheduleRequest
   */
  bool canScheduleRequest(Request& request);

  /**
     \sa SchedulerPolicy::getNextSchedulableRequest
   */
  Request* getNextSchedulableRequest(uint64_t availability);

  /**
     \sa SchedulerPolicy::recordUseTime
   */
  void recordUseTime(uint64_t useTime);

private:
  std::list<Request*> lowPriorityQueue;
  std::list<Request*> highPriorityQueue;

  uint64_t averageUseTime;
  uint64_t numCompletedRequests;
};

#endif // TRITONSORT_MLFQ_POLICY_H
