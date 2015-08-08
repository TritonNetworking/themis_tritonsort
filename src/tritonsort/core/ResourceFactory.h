#ifndef TRITONSORT_RESOURCE_FACTORY_H
#define TRITONSORT_RESOURCE_FACTORY_H

#include "core/Resource.h"
#include "core/TritonSortAssert.h"

/**
   Provides a factory for creating a given type of resource.
 */
class ResourceFactory {
public:
  virtual ~ResourceFactory() {}

  /// Create a new instance of the resource produced by the factory
  /**
     \return a pointer to the newly-created instance
   */
  virtual Resource* newInstance() = 0;

  /**
     Create a new instance of the resource produced by the factory of a given
     size.

     \param size the size of the resource to instantiate

     \return a pointer to the newly-created instance
   */
  virtual Resource* newInstance(uint64_t size) = 0;

  /// Try to create a new resource subject to memory constraints
  /**
     This method is similar to ResourceFactory::newInstance, but returns NULL
     if there is not enough memory to construct a new resource without
     blocking.

     \return a pointer to the newly-created resource, or NULL if there isn't
     enough memory to create the resource at the moment
   */
  virtual Resource* attemptNewInstance() = 0;

  /// Create a new resource subject to memory constraints
  /**
     This method is similar to ResourceFactory::newInstance(uint64_t size), but
     returns NULL if there is not enough memory to construct a new resource
     with the given size without blocking.

     \param size the size of the resource to instantiate

     \return a pointer to the newly-created resource, or NULL if there isn't
     enough memory to create the resource at the moment

   */
  virtual Resource* attemptNewInstance(uint64_t size) = 0;

  /**
     \return the default resource size constructed by this factory
   */
  virtual uint64_t getDefaultSize() = 0;
};

#endif // TRITONSORT_RESOURCE_FACTORY_H
