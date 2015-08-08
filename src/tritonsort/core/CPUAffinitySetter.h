#ifndef THEMIS_CPU_AFFINITY_SETTER_H
#define THEMIS_CPU_AFFINITY_SETTER_H

#include <sched.h>
#include <stdint.h>
#include <string>
#include <vector>

class Params;

/**
   CPUAffinitySetter is responsible for assigning worker threads to a set of
   CPU cores. It works by reading values from the THREAD_CPU_POLICY parameter.

   By default, a thread's CPU affinity is left unspecified and it is allowed to
   be scheduled on any CPU core.

   CPUAffinitySetter assumes that THREAD_CPU_POLICY is a three level deep
   mapping. The first level of the mapping maps phase names to the CPU
   affinities for stages in that phase. The second level of the mapping maps
   stage names to affinity policy information for the workers in that
   stage. The user can also specify a default policy for a stage by using the
   keyword "DEFAULT" instead of a stage name.

   Policy information is defined by two keys: 'mask', whose value is a bit-mask
   describing the set of CPUs over which the policy applies, and 'type', whose
   value is a string that names the policy type being used.

   There are currently two policies defined:
   * 'fixed' : worker x is pinned to core x modulo the number of active cores
     in the mask
   * 'free' : any worker can run on any active core in the mask

   Here's an example THREAD_CPU_POLICY:

   THREAD_CPU_POLICY = {
      phase_one = {
         DEFAULT = {
            type = "free",
            mask = "1111111011111110"
         },
         reader = {
            type = "fixed",
            mask = "0000000100000000"
         },
         writer = {
            type = "fixed",
            mask = "0000000000000001"
         }
      }
   }

   In this policy, stage "reader" in phase "phase_one" schedules all of its
   workers on core 7. Stage "writer" in phase "phase_one" schedules all of its
   workers on core 15. All other stages in phase one are allowed to run on any
   core except for cores 7 and 15. Any other stages are left unspecified, and
   hence can run on any CPU core.

   The number of cores in the system must be specified using the CORES_PER_NODE
   parameter.
 */
class CPUAffinitySetter {
public:
  /// Constructor
  /**
     \param params a Params object that stores parameters used by the
     CPUAffinitySetter to configure itself. This object must persist for the
     lifetime of the CPUAffinitySetter.

     \param phaseName the name of the current phase
   */
  CPUAffinitySetter(Params& params, const std::string& phaseName);

  /**
     Set the CPU affinity mask for a given worker

     \param stageName the name of the worker's parent stage
     \param workerID the worker's unique ID within its stage
     \param[out] outputAffinityMask the affinity mask that will be set with the
     worker's CPU affinities
   */
  void setAffinityMask(
    const std::string& stageName, uint64_t workerID,
    cpu_set_t& outputAffinityMask);

  /// \return the number of cores over which this CPUAffinitySetter is
  /// scheduling threads
  uint64_t getNumCores() const;
private:
  void setAffinityMask(
    uint64_t workerID, const std::string& type, const std::string& mask,
    cpu_set_t& outputAffinityMask);

  void setFixedAffinity(
    const std::vector<int>& desiredMask, uint64_t workerID,
    cpu_set_t& outputAffinityMask);

  void setFreeAffinity(
    const std::vector<int>& desiredMask, uint64_t workerID,
    cpu_set_t& outputAffinityMask);

  const uint64_t numCores;

  const Params& params;
  std::string phaseName;
};

#endif // THEMIS_CPU_AFFINITY_SETTER_H
