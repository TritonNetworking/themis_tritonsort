#ifndef TRITONSORT_BUFFER_LIST_CONTAINER_FACTORY_H
#define TRITONSORT_BUFFER_LIST_CONTAINER_FACTORY_H

#include "common/BufferListContainer.h"
#include "core/ResourceFactory.h"

/**
   A factory that creates new BufferListContainers
 */
template <typename T> class BufferListContainerFactory
  : public ResourceFactory {
public:
  /// \sa ResourceFactory::newInstance
  BufferListContainer<T>* newInstance() {
    return new BufferListContainer<T>();
  }
};

#endif // TRITONSORT_BUFFER_LIST_CONTAINER_FACTORY_H
