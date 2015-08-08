#ifndef THEMIS_DUMMY_WORKER_H
#define THEMIS_DUMMY_WORKER_H

#include "core/BaseWorker.h"
#include "core/TritonSortAssert.h"

/**
   A dummy worker can be used in tests where a parent worker is required but
   the worker doesn't actually have to do anything.
 */
class DummyWorker : public BaseWorker {
public:
  /// Constructor
  /**
     \sa BaseWorker::BaseWorker
   */
  DummyWorker(uint64_t id, const std::string& name);

  /// Aborts immediately, since dummy worker shouldn't process work
  bool processIncomingWorkUnits();
};

#endif // THEMIS_DUMMY_WORKER_H
