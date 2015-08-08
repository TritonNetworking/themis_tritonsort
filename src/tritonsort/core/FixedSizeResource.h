#ifndef TRITONSORT_FIXED_SIZE_RESOURCE_H
#define TRITONSORT_FIXED_SIZE_RESOURCE_H

#include "Resource.h"

/**
   A FixedSizeResource is a Resource that holds a fixed amount of memory. It may
   not be currently using all of the memory, in which case
   Resource::getCurrentSize() will return a value smaller than the max capacity.
 */
class FixedSizeResource : public Resource {
public:
  /// Destructor
  virtual ~FixedSizeResource() {}

  /// Get the capacity of the resource in bytes
  /**
     The capacity of the resource is the fixed maximum number of bytes the
     resource can use. This value should be an upper bound on the current size
     exposed by the Resource base class.

     \return the capacity of this resource in bytes
   */
  virtual uint64_t getCapacity() const = 0;
};

#endif // TRITONSORT_FIXED_SIZE_RESOURCE_H
