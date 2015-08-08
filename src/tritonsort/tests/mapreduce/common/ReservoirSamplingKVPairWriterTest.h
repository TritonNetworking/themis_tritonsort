#ifndef THEMIS_RESERVOIR_SAMPLING_KV_PAIR_WRITER_TEST_H
#define THEMIS_RESERVOIR_SAMPLING_KV_PAIR_WRITER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class ReservoirSamplingKVPairWriterTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( ReservoirSamplingKVPairWriterTest );
  CPPUNIT_TEST( testSetupAndCommitWriteBeforeSampling );
  CPPUNIT_TEST_SUITE_END();
public:
  void testSetupAndCommitWriteBeforeSampling();
};

#endif // THEMIS_RESERVOIR_SAMPLING_KV_PAIR_WRITER_TEST_H
