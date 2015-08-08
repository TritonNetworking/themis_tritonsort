#ifndef MAPRED_PHASE_ZERO_SAMPLE_METADATA_TEST_H
#define MAPRED_PHASE_ZERO_SAMPLE_METADATA_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class PhaseZeroSampleMetadataTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( PhaseZeroSampleMetadataTest );
  CPPUNIT_TEST( testReadAndWriteFromKVPair );
  CPPUNIT_TEST( testMerge );
  CPPUNIT_TEST_SUITE_END();
public:
  void testReadAndWriteFromKVPair();
  void testMerge();
};

#endif // MAPRED_PHASE_ZERO_SAMPLE_METADATA_TEST_H
