#ifndef TRITONSORT_FCFS_POLICY_H
#define TRITONSORT_FCFS_POLICY_H

#include <list>

#include "SchedulerPolicy.h"

/**
   A FCFSPolicy is a scheduler policy that implements first come first serve.
   The policy uses a single FIFO queue to schedule requests. This is the
   simplest scheduling policy.

   FCFS does use resource usage times, so it is possible to use FCFS in
   situations where usage times cannot be accurately tracked. An example
   is a resource pool that offers bulkGet and bulkPut under the assumption
   that groups of resources retrieved with bulkGet() may be split up and
   returned via separate calls to bulkPut().
 */
class FCFSPolicy : public SchedulerPolicy {
public:
  /// Destructor
  virtual ~FCFSPolicy();

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
     FCFS doesn't care about use time.

     \sa SchedulerPolicy::recordUseTime
   */
  void recordUseTime(uint64_t useTime) {}

private:
  std::list<Request*> queue;
};

#endif // TRITONSORT_FCFS_POLICY_H
