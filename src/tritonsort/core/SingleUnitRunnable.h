#ifndef TRITONSORT_SINGLE_UNIT_RUNNABLE_H
#define TRITONSORT_SINGLE_UNIT_RUNNABLE_H

#include "core/BaseWorker.h"
#include "core/Resource.h"
#include "core/ResourceMonitorClient.h"
#include "core/Timer.h"
#include "core/TritonSortAssert.h"

/**
   The base class for all "push-based" workers that accept and process a single
   work unit at a time.

   SingleUnitRunnables are NOT required to check type safety of work units. This
   base class uses template information to ensure type safety with a dynamic
   cast before run() is called.
 */
template <typename T> class SingleUnitRunnable : public BaseWorker {
public:
  /// Constructor
  /**
     \param id the worker's ID
     \param typeName the worker's stage name
   */
  SingleUnitRunnable(uint64_t id, const std::string& typeName)
    : BaseWorker(id, typeName) {
    logWorkerType("push");
  }

  /// Destructor
  virtual ~SingleUnitRunnable() {
  }

  /// The main body of the worker
  /**
     This method is called when the worker has to process a work unit. It's
     assumed that the worker will either pass the work unit on to another
     stage, return it to a ResourcePool or destruct it sometime during this
     call.

     \param workUnit the work unit to process
   */
  virtual void run(T* workUnit) = 0;

  /**
     \sa BaseWoker::processCurrentWorkQueue
   */
  bool processIncomingWorkUnits() {

    startWaitForWorkTimer();
    Resource* workUnit = tracker->getNewWork(getID());
    stopWaitForWorkTimer();

    if (workUnit != NULL) {
      T* typedWorkUnit = dynamic_cast<T*>(workUnit);
      ABORT_IF(typedWorkUnit == NULL, "Got NULL or incorrectly typed work "
               "unit.");

      logConsumed(typedWorkUnit);

      logInputSize(workUnit->getCurrentSize());

      this->startRuntimeTimer();

      run(typedWorkUnit);

      this->stopRuntimeTimer();
      this->logRuntimeInformation();
      this->resetRuntimeInformation();
    }

    return workUnit == NULL;
  }
};

#endif // TRITONSORT_SINGLE_UNIT_RUNNABLE_H
