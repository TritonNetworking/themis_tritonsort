#ifndef TRITONSORT_RESOURCE_H
#define TRITONSORT_RESOURCE_H

/**
   Basic interface that all resources must implement. Since this touches lots
   of things, it should be kept as small as possible.
 */

#include <stdint.h>

class Resource {
public:
  /// Destructor
  virtual ~Resource() {}

  /// Get the size of the resource in bytes
  /**
     The size of a resource could be the amount of memory it's using, or the
     size of the file it's pointing to, or any one of a number of things
     depending on the particular resource.

     \return the size of this resource in bytes
   */
  virtual uint64_t getCurrentSize() const = 0;
};

#endif // TRITONSORT_RESOURCE_H
