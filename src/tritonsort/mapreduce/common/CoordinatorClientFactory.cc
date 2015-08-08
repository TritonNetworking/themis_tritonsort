#include "core/Params.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/DebugCoordinatorClient.h"
#include "mapreduce/common/RedisCoordinatorClient.h"

CoordinatorClientInterface* CoordinatorClientFactory::newCoordinatorClient(
  const Params& params, const std::string& phaseName, const std::string& role,
  uint64_t id) {

  const std::string& coordinatorClientName = params.get<std::string>(
    "COORDINATOR_CLIENT");

  if (coordinatorClientName.compare("redis") == 0) {
    return new RedisCoordinatorClient(params, phaseName, role, id);
  } else if (coordinatorClientName.compare("debug") == 0) {
    return new DebugCoordinatorClient(params, phaseName, id);
  } else if (coordinatorClientName.compare("none") == 0) {
    return NULL;
  } else {
    ABORT("Unknown coordinator client name '%s'",
          coordinatorClientName.c_str());
    return NULL;
  }
}
