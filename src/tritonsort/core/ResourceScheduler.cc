#include <algorithm>
#include <new>

#include "ResourceScheduler.h"
#include "TritonSortAssert.h"

ResourceScheduler::ResourceScheduler(uint64_t _capacity,
                                     SchedulerPolicy& _policy,
                                     pthread_mutex_t& _sharedLock,
                                     bool _useTrackingCookies, bool _testMode)
  : capacity(_capacity),
    availability(_capacity),
    useTrackingCookies(_useTrackingCookies),
    policy(_policy),
    sharedLock(_sharedLock),
    testMode(_testMode) {
}

ResourceScheduler::~ResourceScheduler() {
  // Destroy and delete any CVs created at runtime.
  for (std::map<const void*, pthread_cond_t*>::iterator iter =
         conditionVariables.begin();
       iter != conditionVariables.end(); ++iter) {
    pthread_cond_destroy(iter->second);
    delete iter->second;
  }

  // All resources and cookies should be returned.
  ASSERT(availability == capacity,
         "All resources should be available when the resource scheduler is "
         "destroyed, but only %llu of %llu are available.",
         availability, capacity);
  ASSERT(cookies.empty(),
         "All cookies should be returned when the resource scheduler is "
         "destroyed, but there are still %llu outstanding.", cookies.size());
}

void ResourceScheduler::_schedule(uint64_t size, const void* caller) {
  ABORT_IF(size > capacity,
           "Request size %llu is greater than total resource capacity %llu. "
           "Consider increasing resource capacity.", size, capacity);

  // Lazily create a condition variable.
  if (!conditionVariables.count(caller)) {
    pthread_cond_t* newCV = new (std::nothrow) pthread_cond_t;
    ABORT_IF(newCV == NULL,
             "Ran out of memory while allocating a condition variable.");
    pthread_cond_init(newCV, NULL);
    conditionVariables[caller] = newCV;
  }

  // Create a request and add to the policy.
  // It is safe to allocate the request locally because it is not needed after
  // this function returns.
  SchedulerPolicy::Request request(caller, size);
  policy.addRequest(request);

  // Block until the request can be scheduled.
  while (availability < request.size || !policy.canScheduleRequest(request)) {
    if (testMode) {
      // The scheduler is used in a unit test, so force the request to be
      // removed, and abort so the test can detect the block.
      policy.removeRequest(request, true);
      ABORT("_schedule() blocked during unit test.");
    }
    // Wait until the request can be scheduled.
    pthread_cond_wait(conditionVariables[caller], &sharedLock);
  }

  // Update availability and remove the request from policy queues.
  availability -= size;
  policy.removeRequest(request);

  // Wake up a request that can be scheduled now that this one has completed.
  tryWakeBlockedRequest();
}

void ResourceScheduler::schedule(uint64_t size, const void* caller) {
  ASSERT(!useTrackingCookies,
         "Cannot call schedule() when using tracking cookies.");
  _schedule(size, caller);
}

const void* ResourceScheduler::scheduleWithCookie(uint64_t size,
                                                  const void* caller) {
  ASSERT(useTrackingCookies, "Cannot call scheduleWithCookie() unless tracking "
         "cookies are enabled.");
  // Schedule the resource request. This call will only return after the request
  // is scheduled.
  _schedule(size, caller);

  // Create a return cookie for the caller.
  Cookie* cookie = new (std::nothrow) Cookie(size);
  ABORT_IF(cookie == NULL,
           "Ran out of memory while allocating a cookie.");
  cookies.push_back(cookie);

  return cookie;
}

void ResourceScheduler::_release(uint64_t size) {
  // Update availability.
  availability += size;
  ASSERT(availability <= capacity,
         "Somehow completing a scheduled resource request, resulted in an "
         "availability of %llu, but capacity is only %llu.", availability,
         capacity);

  // Wake up a request that can be scheduled now that resources have been freed.
  tryWakeBlockedRequest();
}

void ResourceScheduler::release(uint64_t size) {
  ASSERT(!useTrackingCookies,
         "Cannot call release() when using tracking cookies.");
  _release(size);
}

void ResourceScheduler::releaseWithCookie(const void* cookie) {
  ASSERT(useTrackingCookies, "Cannot call releaseWithCookie() unless tracking "
         "cookies are enabled.");
  // Although this cast is technically unsafe, the subsequent ABORT will detect
  // a bogus pointer, so it should be OK.
  Cookie* cookiePtr = reinterpret_cast<Cookie*>(const_cast<void*>(cookie));
  // Check to see if the cookie pointer is known. In particular, if the void*
  // was not a Cookie* to begin with, then this ABORT will trigger.
  ABORT_IF(std::find(
             cookies.begin(), cookies.end(), cookiePtr) == cookies.end(),
           "releaseWithCookie() received an unknown cookie. Must pass cookie "
           "created with scheduleWithCookie()");

  // Release the resources associated with this cookie.
  _release(cookiePtr->size);

  // Let the policy know about the usage time.
  uint64_t now = Timer::posixTimeInMicros();
  policy.recordUseTime(now - cookiePtr->timestamp);

  // Remove and delete the cookie.
  cookies.remove(cookiePtr);
  delete cookiePtr;
}

uint64_t ResourceScheduler::getAvailability() {
  return availability;
}

void ResourceScheduler::tryWakeBlockedRequest() {
  // Ask the policy if it has any requests that can be scheduled now.
  SchedulerPolicy::Request* request =
    policy.getNextSchedulableRequest(availability);
  if (request != NULL) {
    // Wake the thread.
    pthread_cond_signal(conditionVariables[request->caller]);
  }
}
