#ifndef THEMIS_TEST_MULTI_DESTINATION_WORKER_H
#define THEMIS_TEST_MULTI_DESTINATION_WORKER_H

#include <string>

#include "core/SingleUnitRunnable.h"
#include "tests/themis_core/StringWorkUnit.h"

class MultiDestinationWorker
  : public SingleUnitRunnable<StringWorkUnit> {
WORKER_IMPL

public:
  MultiDestinationWorker(uint64_t id, const std::string& stageName);

  void run(StringWorkUnit* workUnit);

  void handleNamedDownstreamTracker(
    const std::string& trackerDescription, uint64_t trackerID);

private:
  std::map<std::string, uint64_t> stringToTrackerIDMap;
};

#endif // THEMIS_TEST_MULTI_DESTINATION_WORKER_H
