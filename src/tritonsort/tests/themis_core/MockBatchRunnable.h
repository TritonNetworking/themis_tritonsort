#ifndef TRITONSORT_MOCK_BATCH_RUNNABLE_H
#define TRITONSORT_MOCK_BATCH_RUNNABLE_H

#include "core/BatchRunnable.h"

class MockBatchRunnable : public BatchRunnable {
WORKER_IMPL

public:
  MockBatchRunnable(uint64_t id, const std::string& name);
  void run(WorkQueue& workQueue);
  uint64_t getNumWorkUnitsProcessed();
private:
  uint64_t workUnitsProcessed;
};

#endif // TRITONSORT_MOCK_BATCH_RUNNABLE_H
