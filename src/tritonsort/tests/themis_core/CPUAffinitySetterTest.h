#ifndef TRITONSORT_CPU_AFFINITY_SETTER_TEST_H
#define TRITONSORT_CPU_AFFINITY_SETTER_TEST_H

#include <set>
#include <stdint.h>

#include "third-party/googletest.h"

class CPUAffinitySetterTest : public ::testing::Test {
protected:
  // Assert that only the CPUs present in the cpus vector are set
  // in affinityMask
  void assertCPUsSet(cpu_set_t& affinityMask, uint64_t numCPUs,
                     std::set<int>& cpus);
};

#endif // TRITONSORT_CPU_AFFINITY_SETTER_TEST_H
