#include "core/CPUAffinitySetter.h"
#include "core/Params.h"

CPUAffinitySetter::CPUAffinitySetter(
  Params& _params, const std::string& _phaseName)
  : numCores(_params.get<uint64_t>("CORES_PER_NODE")),
    params(_params),
    phaseName(_phaseName) {
}

void CPUAffinitySetter::setAffinityMask(
  const std::string& stageName, uint64_t workerID,
  cpu_set_t& outputAffinityMask) {

  CPU_ZERO(&outputAffinityMask);

  std::string baseParamName("THREAD_CPU_POLICY." + phaseName + "." + stageName);

  std::string maskParamName(baseParamName + ".mask");
  std::string typeParamName(baseParamName + ".type");

  if (!params.contains(maskParamName)) {
    // Make sure the user hasn't defined a mask without a type
    ABORT_IF(params.contains(typeParamName), "Must specify a parameter '%s' "
             "to go with parameter '%s'", typeParamName.c_str(),
             maskParamName.c_str());

    // Check for the existence of a default policy for the phase
    std::string defaultBaseParamName(
      "THREAD_CPU_POLICY." + phaseName + ".DEFAULT");
    std::string defaultMaskParamName(defaultBaseParamName + ".mask");
    std::string defaultTypeParamName(defaultBaseParamName + ".type");

    if (params.contains(defaultTypeParamName) &&
        params.contains(defaultMaskParamName)) {

      const std::string& type = params.get<std::string>(defaultTypeParamName);
      const std::string& mask = params.get<std::string>(defaultMaskParamName);

      setAffinityMask(workerID, type, mask, outputAffinityMask);
    } else {
      // The user hasn't specified anything, so fall back to the "anything
      // goes" policy, allowing the thread to be scheduled on any core

      for (uint64_t i = 0; i < numCores; i++) {
        CPU_SET(i, &outputAffinityMask);
      }
    }
  } else {
    // Make sure the user hasn't defined a type without a mask
    ABORT_IF(!params.contains(maskParamName), "Must specify a parameter '%s' "
             "to go with parameter '%s'", maskParamName.c_str(),
             typeParamName.c_str());

    const std::string& type = params.get<std::string>(typeParamName);
    const std::string& mask = params.get<std::string>(maskParamName);

    setAffinityMask(workerID, type, mask, outputAffinityMask);
  }
}

uint64_t CPUAffinitySetter::getNumCores() const {
  return numCores;
}

void CPUAffinitySetter::setAffinityMask(
  uint64_t workerID, const std::string& type, const std::string& mask,
  cpu_set_t& outputAffinityMask) {

  ABORT_IF(mask.length() != numCores, "Expected core masks to be %llu bits "
           "long, but '%s' is a %llu-bit mask", numCores,
           mask.c_str(), mask.length());

  std::vector<int> maskBits;

  for (uint64_t maskIndex = 0; maskIndex < numCores; maskIndex++) {
    if (mask[maskIndex] == '1') {
      maskBits.push_back(maskIndex);
    }
  }

  if (type == "fixed") {
    setFixedAffinity(maskBits, workerID, outputAffinityMask);
  } else if (type == "free") {
    setFreeAffinity(maskBits, workerID, outputAffinityMask);
  } else {
    ABORT("Unknown CPU affinity policy type '%s'", type.c_str());
  }
}

void CPUAffinitySetter::setFixedAffinity(
  const std::vector<int>& desiredMask, uint64_t workerID,
  cpu_set_t& outputAffinityMask) {

  CPU_SET(desiredMask[workerID % desiredMask.size()], &outputAffinityMask);
}

void CPUAffinitySetter::setFreeAffinity(
  const std::vector<int>& desiredMask, uint64_t workerID,
  cpu_set_t& outputAffinityMask) {

  for (std::vector<int>::const_iterator iter = desiredMask.begin();
       iter != desiredMask.end(); iter++) {
    CPU_SET(*iter, &outputAffinityMask);
  }
}
