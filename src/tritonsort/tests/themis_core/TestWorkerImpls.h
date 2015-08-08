#ifndef TRITONSORT_TESTS_TEST_WORKER_IMPLS_H
#define TRITONSORT_TESTS_TEST_WORKER_IMPLS_H

#include "core/ImplementationList.h"
#include "tests/themis_core/Countdown.h"
#include "tests/themis_core/Emitter.h"
#include "tests/themis_core/ManualConsumerWorker.h"
#include "tests/themis_core/MemoryEatingWorker.h"
#include "tests/themis_core/MockBatchRunnable.h"
#include "tests/themis_core/MultiDestinationWorker.h"

class TestWorkerImpls : public ImplementationList {
public:
  TestWorkerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(MockBatchRunnable, "MockBatchRunnable");
    ADD_IMPLEMENTATION(Emitter, "Emitter");
    ADD_IMPLEMENTATION(Countdown, "Countdown");
    ADD_IMPLEMENTATION(MemoryEatingWorker, "MemoryEatingWorker");
    ADD_IMPLEMENTATION(ManualConsumerWorker, "ManualConsumerWorker");
    ADD_IMPLEMENTATION(MultiDestinationWorker, "MultiDestinationWorker");
  }
};

#endif // TRITONSORT_TESTS_TEST_WORKER_IMPLS_H
