#ifndef THEMIS_SINK_WRITER_H
#define THEMIS_SINK_WRITER_H

#include "core/SingleUnitRunnable.h"
#include "common/WriteTokenPool.h"

template <typename T> class SinkWriter
  : public SingleUnitRunnable<T> {
WORKER_IMPL

public:
  SinkWriter(
    uint64_t id, const std::string& stageName, WriteTokenPool& _writeTokenPool)
    : SingleUnitRunnable<T>(id, stageName),
      writeTokenPool(_writeTokenPool) {
  }

  void run(T* resource) {
    WriteToken* token = resource->getToken();

    if (token != NULL) {
      writeTokenPool.putToken(token);
    }

    delete resource;
  }

private:
  WriteTokenPool& writeTokenPool;
};

template <typename T> BaseWorker* SinkWriter<T>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  WriteTokenPool* writeTokenPool = dependencies.get<WriteTokenPool>(
    "write_token_pool");

  SinkWriter* sink = new SinkWriter(id, stageName, *writeTokenPool);

  return sink;
}

#endif // THEMIS_SINK_WRITER_H
