#ifndef TRITONSORT_ITEM_HOLDER_H
#define TRITONSORT_ITEM_HOLDER_H

#include <list>

#include "core/None.h"
#include "core/SingleUnitRunnable.h"

/**
   Designed to be the sink of a pipeline where the end result is an in-memory
   data structure or data structures that are to be retrieved at the end of the
   run, possibly so that they can be passed on to a subsequent phase.

   \warning Be careful to garbage-collect the items held in this worker, since
   they won't be automatically garbage collected.
 */
template <typename T> class ItemHolder : public SingleUnitRunnable<T> {
WORKER_IMPL
public:
  /// Constructor
  /**
     \param id the worker's ID
     \param name the name of the stage for which this worker is working
   */
  ItemHolder(uint64_t id, const std::string& name)
    : SingleUnitRunnable<T>(id, name) {
  }

  /**
     Holds onto the given work unit by placing it in an internal list

     \param workUnit a work unit to hold onto
   */
  void run(T* workUnit) {
    heldItems.push_back(workUnit);
  }

  /**
     Get the list of work units "held" by this worker

     \return a reference to the worker's internal held work units list
   */
  const std::list<T*>& getHeldItems() const {
    return heldItems;
  }
private:
  std::list<T*> heldItems;
};

template <typename T> BaseWorker* ItemHolder<T>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  ItemHolder<T>* itemHolder = new ItemHolder<T>(id, stageName);
  return itemHolder;
}

#endif // TRITONSORT_ITEM_HOLDER_H
