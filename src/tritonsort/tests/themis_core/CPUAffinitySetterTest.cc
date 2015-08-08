#include <boost/filesystem.hpp>
#include <stdarg.h>
#include <string>

#include "CPUAffinitySetterTest.h"
#include "core/CPUAffinitySetter.h"
#include "core/Params.h"

extern const char* TEST_BINARY_ROOT;

void CPUAffinitySetterTest::assertCPUsSet(
  cpu_set_t& affinityMask, uint64_t numCPUs, std::set<int>& cpus) {

  for (uint64_t currentCPU = 0; currentCPU < numCPUs; currentCPU++) {
    if (cpus.count(currentCPU)) {
      CPPUNIT_ASSERT(CPU_ISSET(currentCPU, &affinityMask));
    } else {
      CPPUNIT_ASSERT(!CPU_ISSET(currentCPU, &affinityMask));
    }
  }
}


void CPUAffinitySetterTest::testFixedPolicy() {
  uint64_t numCores = 16;

  Params params;

  params.add("CORES_PER_NODE", numCores);
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.type", "fixed");

  // Set CPUs 8, 10 and 13 ( range is [0,numCores) )
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.mask", "0000000010100100");

  CPUAffinitySetter cpuAffinitySetter(params, "test_phase");

  cpu_set_t affinityMask;

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 0, affinityMask);

  std::set<int> setCPUs;
  setCPUs.insert(8);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 1, affinityMask);
  setCPUs.insert(10);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 2, affinityMask);
  setCPUs.insert(13);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 3, affinityMask);
  setCPUs.insert(8);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();
}

void CPUAffinitySetterTest::testFreePolicy() {
  uint64_t numCores = 16;

  Params params;

  params.add("CORES_PER_NODE", numCores);
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.type", "free");

  // Set CPUs 8, 10 and 13 ( range is [0,numCores) )
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.mask", "0000000010100100");

  CPUAffinitySetter cpuAffinitySetter(params, "test_phase");

  cpu_set_t affinityMask;

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 0, affinityMask);

  std::set<int> setCPUs;
  setCPUs.insert(8);
  setCPUs.insert(10);
  setCPUs.insert(13);
  assertCPUsSet(affinityMask, numCores, setCPUs);
}

void CPUAffinitySetterTest::testParameterParsing() {
  uint64_t numCores = 16;

  Params params;
  boost::filesystem::path configFilePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" /
    "parameter_parsing_test_file.yaml");

  params.parseFile(configFilePath.string().c_str());
  params.add("CORES_PER_NODE", numCores);

  CPUAffinitySetter cpuAffinitySetter(params, "test_phase");

  std::set<int> setCPUs;

  cpu_set_t affinityMask;

  CPPUNIT_ASSERT_THROW(
    cpuAffinitySetter.setAffinityMask("weird_policy", 0, affinityMask),
    AssertionFailedException);
  CPPUNIT_ASSERT_THROW(
    cpuAffinitySetter.setAffinityMask("maskless", 0, affinityMask),
    AssertionFailedException);
  CPPUNIT_ASSERT_THROW(
    cpuAffinitySetter.setAffinityMask("typeless", 0, affinityMask),
    AssertionFailedException);
  CPPUNIT_ASSERT_THROW(
    cpuAffinitySetter.setAffinityMask("bad_mask", 0, affinityMask),
    AssertionFailedException);

  cpuAffinitySetter.setAffinityMask("reader", 0, affinityMask);
  setCPUs.insert(4);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();
  cpuAffinitySetter.setAffinityMask("reader", 1, affinityMask);
  setCPUs.insert(8);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();

  cpuAffinitySetter.setAffinityMask("sender", 0, affinityMask);
  setCPUs.insert(4);
  setCPUs.insert(6);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();

  cpuAffinitySetter.setAffinityMask("sender", 7, affinityMask);
  setCPUs.insert(4);
  setCPUs.insert(6);
  assertCPUsSet(affinityMask, numCores, setCPUs);
  setCPUs.clear();
}

void CPUAffinitySetterTest::testPhaseDefaultPolicy() {
  uint64_t numCores = 16;

  Params params;

  params.add("CORES_PER_NODE", numCores);
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.type", "free");
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.MyWorkerType.mask", "0000000010100100");
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.DEFAULT.type", "free");
  params.add<std::string>(
    "THREAD_CPU_POLICY.test_phase.DEFAULT.mask", "0101010101010101");

  CPUAffinitySetter cpuAffinitySetter(params, "test_phase");

  // Make sure defined affinities still function properly
  cpu_set_t affinityMask;

  cpuAffinitySetter.setAffinityMask("MyWorkerType", 0, affinityMask);

  std::set<int> setCPUs;
  setCPUs.insert(8);
  setCPUs.insert(10);
  setCPUs.insert(13);
  assertCPUsSet(affinityMask, numCores, setCPUs);

  // Make sure an unspecified worker type in this phase triggers the default
  // policy
  cpuAffinitySetter.setAffinityMask("OtherWorkerType", 0, affinityMask);

  setCPUs.clear();
  setCPUs.insert(1);
  setCPUs.insert(3);
  setCPUs.insert(5);
  setCPUs.insert(7);
  setCPUs.insert(9);
  setCPUs.insert(11);
  setCPUs.insert(13);
  setCPUs.insert(15);
  assertCPUsSet(affinityMask, numCores, setCPUs);
}
