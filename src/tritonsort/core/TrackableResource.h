#ifndef TRITONSORT_TRACKABLE_RESOURCE_H
#define TRITONSORT_TRACKABLE_RESOURCE_H

#include "Resource.h"

/**
   A TrackableResource is a resource that contains a cookie for tracking
   purposes. Any resource pool that uses a scheduler with cookies should
   probably be using TrackableResources.
 */
class TrackableResource : public Resource {
public:
  /// Constructor
  /**
     \param _cookie a tracking cookie for this resource.
   */
  TrackableResource(const void* const _cookie) :
    cookie(_cookie) {}

  /**
     \return the tracking cookie for this resource
   */
  const void* const getCookie() {
    return cookie;
  }

private:
  const void* const cookie;
};

#endif // TRITONSORT_TRACKABLE_RESOURCE_H
