#ifndef THEMIS_PROVENANCE_TEST_SUITE_H
#define THEMIS_PROVENANCE_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "tests/mapreduce/common/provenance/SourceFileRangesSetTest.h"

class ProvenanceTestSuite : public CppUnit::TestSuite {
public:
  ProvenanceTestSuite() : CppUnit::TestSuite("MapREduce provenance") {
    addTest(SourceFileRangesSetTest::suite());
  }
};

#endif // THEMIS_PROVENANCE_TEST_SUITE_H
