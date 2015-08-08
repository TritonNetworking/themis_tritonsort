#include "common/DummyWorker.h"

DummyWorker::DummyWorker(uint64_t id, const std::string& name)
  : BaseWorker(id, name) {
}

bool DummyWorker::processIncomingWorkUnits() {
  ABORT("Shouldn't ask a dummy worker to process work units");
  return true;
}
