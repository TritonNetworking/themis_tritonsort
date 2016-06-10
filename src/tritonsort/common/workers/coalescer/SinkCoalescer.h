#ifndef TRITONSORT_SINK_COALESCER_H
#define TRITONSORT_SINK_COALESCER_H

#include <limits.h>

#include "common/buffers/BufferList.h"
#include "common/BufferListContainer.h"
#include "common/WriteTokenPool.h"
#include "core/SingleUnitRunnable.h"

/**
   A sink implementation of the Coalescer stage
 */
template <typename In> class SinkCoalescer
  : public SingleUnitRunnable<BufferListContainer<In> >  {
WORKER_IMPL

public:
  /// Shorthand for a BufferListContainer for the appropriate LD buffer type
  typedef BufferListContainer<In> IBLContainer;

  /// Constructor
  /**
     \param id the worker ID for this worker

     \param name the name of the stage for which the worker is working

     \param _writeTokenPool a pool of write tokens to which the container's
     write token will be returned
   */
  SinkCoalescer(
    uint64_t id, const std::string& name, WriteTokenPool& _writeTokenPool)
    : SingleUnitRunnable<IBLContainer>(id, name),
      writeTokenPool(_writeTokenPool) {
  }

  /**
     A SinkCoalescer returns all buffers and tokens contained in the given
     container to their respective buffer pools.

     \param container the container to be processed
   */
  void run(IBLContainer* container) {
    BufferList<In>& list = container->getList();

    WriteToken* token = container->getToken();

    if (token != NULL) {
      writeTokenPool.putToken(token);
    }

    In* head = list.removeHead();

    while (head != NULL) {
      delete head;
      head = list.removeHead();
    }

    delete container;
  }

private:
  WriteTokenPool& writeTokenPool;
};

template <typename In> BaseWorker* SinkCoalescer<In>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  WriteTokenPool* writeTokenPool = dependencies.get<WriteTokenPool>(
    "write_token_pool");

  SinkCoalescer<In>* coalescer = new SinkCoalescer<In>(
    id, stageName, *writeTokenPool);

  return coalescer;
}

#endif // TRITONSORT_SINK_COALESCER_H
