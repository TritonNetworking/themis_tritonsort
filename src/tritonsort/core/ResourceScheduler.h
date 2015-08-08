#ifndef TRITONSORT_RESOURCE_SCHEDULER_H
#define TRITONSORT_RESOURCE_SCHEDULER_H

#include <list>
#include <map>
#include <pthread.h>
#include <utility>

#include "SchedulerPolicy.h"

/**
   A ResourceScheduler is a synchronization mechanism for scheduling requests
   to a limited collection of resources. If the user attempts to schedule a
   resource request, but there are not enough resources available, the request
   will block until resources are freed up. A separate SchedulerPolicy object
   determines when blocked requests become unblocked. Examples of policies are
   multi level feedback queue, and first come first serve.

   Resource schedulers do not allocate resources themselves. The caller is
   responsible for using creating resources in tandem with scheduling
   decisions. This leaves the caller the flexibility of retrieving resources
   from a pre-allocated free list, or creating them dynamically, or a
   combination of the two.

   A ResourceScheduler requires the user to perform allocations and scheduling
   commands in a particular order. The usage patterns are as follows:

   // ===
   // Scheduling:
   // ===
   {
     pthread_mutex_lock(&sharedLock);

     schedule(size, caller);

     // Allocate buffer here
     // ..

     pthread_mutex_unlock(&sharedLock);
   }

   // ===
   // Releasing:
   // ===
   {
     pthread_mutex_lock(&sharedLock);

     // Deallocate buffer here
     // ..

     release(size);

     pthread_mutex_unlock(&sharedLock);
   }


   A shared lock is used for synchronization. If a schedule() call must block,
   the scheduler will wait on a condition variable and release the shared lock.
   The shared lock programming paradigm is annoying, but is required to preserve
   the atomicity requirements of resource scheduling and allocation.

   For example, if the caller wants to issue a schedule() request for the
   maximum number of available resources, atomicity is required to prevent
   resources from being taken by another thread after the request size is
   calculated, but before the request is made:

   Thread A --->  uint64_t requestSize = getAvailability();
                  uint64_t requestSize = getAvailability();  <--- Thread B
                  schedule(requestSize);                     <--- Thread B
   Thread A --->  schedule(requestSize);

   In this case, Thread A's request will block for potentially a long time if
   the request size happens to be large.  Performing calculations atomically
   with scheduling requests avoids this issue, since the loser of the race will
   ask for a smaller request size and will not block as long.


   The ResourceScheduler provides two API usages. The simplest version does not
   track resource usage:

   schedule(size, caller);
   release(size);

   The second API uses cookies to track resource usage:

   const void* const cookie = scheduleWithCookie(size, caller);
   releaseWithCookie(cookie);

   You CANNOT mix and match APIs. A boolean provided to the constructor enables
   one API or the other.

   Certain scheduling policies, such as MLFQ, require cookies in order to
   function as intended. The use of cookies may not be possible in every
   situation. For example, if resources are subdivided or aggregated, then
   usage cannot be accurately tracked with cookies. In particular, any resource
   pool that implements bulkGet() and bulkPut() will most likely not be able to
   use cookies.
 */
class ResourceScheduler {
public:
  /// Constructor
  /**
     \param capacity the total number of resources tracked by the scheduler

     \param policy the scheduling policy to use

     \param sharedLock a lock taken by the caller before calling into the
     scheduler

     \param useTrackingCookies flag that determines whether the tracking cookie
     API is to be used or not

     \param testMode true if the scheduler is constructed for unit tests.
     Defaults to false.
   */
  ResourceScheduler(uint64_t capacity, SchedulerPolicy& policy,
                    pthread_mutex_t& sharedLock, bool useTrackingCookies,
                    bool testMode=false);

  /// Destructor
  virtual ~ResourceScheduler();

  /**
     Schedule a resource request of a particular size. A caller is used to
     distinguish calls from separate threads for blocking purposes. The only
     restriction is that separate threads needs to use a separate caller
     argument in order for the scheduler to properly synchronize requests.

     \warning You must take the sharedLock before calling this method.

     \param size the number of resources used by this request

     \param caller a distinguishing identifier for thread synchronization
   */
  void schedule(uint64_t size, const void* caller);

  /**
     Schedule a resource request and return a cookie as a receipt of the
     scheduling decision. Cookies are used to track usage times. This version of
     schedule() cannot be used if resources cannot be accurately tracked, as in
     the case of a resource pool offering bulkGet() and bulkPut() functionality.
     The corresponding releaseWithCookie() must be used with this function.

     \warning You must take the sharedLock before calling this method.

     \param size the number of resources used by this request

     \param caller a distinguishing identifier for thread synchronization

     \return a cookie corresponding to the scheduling of this resource
   */
  const void* scheduleWithCookie(uint64_t size, const void* caller);

  /**
     Release resources that were scheduled using schedule().

     \warning You must take the sharedLock before calling this method.

     \param size the number of resources to release
   */
  void release(uint64_t size);

  /**
     Release resources that were scheduled using scheduleWithCookie().

     \warning You must take the sharedLock before calling this method.

     \param cookie the receipt cookie associated with the resource scheduling
     decision
   */
  void releaseWithCookie(const void* cookie);

  /**
     \warning You must take the sharedLock before calling this method.

     \return the number of available resources
   */
  uint64_t getAvailability();

private:
  /**
     Helper function that implements the majority of the scheduling logic.

     \param size the number of resources used by this request

     \param caller a distinguishing identifier for thread synchronization
   */
  void _schedule(uint64_t size, const void* caller);

  /**
     Helper function that implements the majority of the release logic.

     \param size the number of resources to release
   */
  void _release(uint64_t size);

  /**
     Check with the policy for a request that can be scheduled and unblock it.
   */
  void tryWakeBlockedRequest();

  /**
     A Cookie is an internal data structure used to track resource usage. Cookie
     is exposed outside the class only as a void*. The actual Cookie
     implementation pertains only to this class.
   */
  struct Cookie {
    uint64_t size;
    uint64_t timestamp;
    Cookie(uint64_t _size)
      : size(_size),
        timestamp(Timer::posixTimeInMicros()) {}
  };

  uint64_t capacity;
  uint64_t availability;
  bool useTrackingCookies;
  SchedulerPolicy& policy;

  std::list<Cookie*> cookies;

  // Locks
  pthread_mutex_t& sharedLock;
  std::map<const void*, pthread_cond_t*> conditionVariables;

  // Testing variables
  bool testMode;
};

#endif // TRITONSORT_RESOURCE_SCHEDULER_H
