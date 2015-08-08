#ifndef TRITONSORT_SCHEDULER_POLICY_H
#define TRITONSORT_SCHEDULER_POLICY_H

#include "Timer.h"
/**
   A SchedulerPolicy implements the logic that determines when requests made to
   a ResourceScheduler can be fulfilled. The ResourceScheduler enforces the
   requirement that there be enough resources available to fulfill a request,
   but otherwise leaves policy decisions up to the SchedulerPolicy.

   A variety of policies are possible with this interface.  For example, first
   come first serve can be implemented using a single FIFO queue.
 */
class SchedulerPolicy {
public:
  /**
     A Request is a data structure that represents a resource request that has
     yet to be scheduled. Requests are constructed in the ResourceScheduler, but
     they are stored in the policy, which determines when they can be scheduled.
   */
  struct Request {
    const void* caller;
    uint64_t size;
    uint64_t timestamp;
    Request(const void* _caller, uint64_t _size)
      : caller(_caller),
        size(_size),
        timestamp(Timer::posixTimeInMicros()) {}
  };

  /// Destructor
  virtual ~SchedulerPolicy() {}

  /**
     Add a resource to the policy's internal data structures.

     \param request the request to add
   */
  virtual void addRequest(Request& request)=0;

  /**
     Remove a request from the policy's internal data structures. If force is
     set to true, the policy should remove the request regardless of whether or
     not it is in a valid position to be scheduled.

     \param request the request to remove

     \param force true if the request should be unconditionally removed
   */
  virtual void removeRequest(Request& request, bool force=false)=0;

  /**
     Predicate that determines if a particular request can be scheduled. The
     resource availability requirement is enforced in the ResourceScheduler, so
     it does NOT need to be checked here.

     \param request the request to be checked for schedulability

     \return true if the request can be servced now and false otherwise
   */
  virtual bool canScheduleRequest(Request& request)=0;

  /**
     Find the next request that can be scheduled using this policy. The policy
     WILL need to check if there are enough available resources here.

     \param availability the resource scheduler's current resource availability

     \return the next request that can be scheduled, or NULL if no requests can
     be scheduled.
   */
  virtual Request* getNextSchedulableRequest(uint64_t availability)=0;

  /**
     Records the time between a resource request being scheduled and the return
     of the resources to the scheduler. If the policy cares about this value, it
     can do some computation here. Otherwise the policy can always ignore the
     usage times.

     \param useTime the time that a resource was in use before being returned
  */
  virtual void recordUseTime(uint64_t useTime)=0;
};

#endif // TRITONSORT_SCHEDULER_POLICY_H
