#ifndef TRITONSORT_CPU_AFFINITY_SETTER_TEST_H
#define TRITONSORT_CPU_AFFINITY_SETTER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <set>
#include <stdint.h>

class CPUAffinitySetterTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( CPUAffinitySetterTest );
  CPPUNIT_TEST( testFixedPolicy );
  CPPUNIT_TEST( testFreePolicy );
  CPPUNIT_TEST( testParameterParsing );
  CPPUNIT_TEST( testPhaseDefaultPolicy );
  CPPUNIT_TEST_SUITE_END();

public:
  void testFixedPolicy();
  void testFreePolicy();
  void testParameterParsing();
  void testPhaseDefaultPolicy();

private:
  // Assert that only the CPUs present in the cpus vector are set
  // in affinityMask
  void assertCPUsSet(cpu_set_t& affinityMask, uint64_t numCPUs,
                     std::set<int>& cpus);
};

#endif // TRITONSORT_CPU_AFFINITY_SETTER_TEST_H
