#ifndef MAPRED_COMMON_TEST_SUITE_H
#define MAPRED_COMMON_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "tests/mapreduce/common/BoundaryListTest.h"
#include "tests/mapreduce/common/DiskBackedBoundaryKeyListTest.h"
#include "tests/mapreduce/common/FilenameToStreamIDMapTest.h"
#include "tests/mapreduce/common/KVPairBufferTest.h"
#include "tests/mapreduce/common/KVPairFormatReaderTest.h"
#include "tests/mapreduce/common/KVPairWriterTest.h"
#include "tests/mapreduce/common/KeyValuePairTest.h"
#include "tests/mapreduce/common/PhaseZeroSampleMetadataTest.h"
#include "tests/mapreduce/common/ReadRequestTest.h"
#include "tests/mapreduce/common/RecordFilterTest.h"
#include "tests/mapreduce/common/ReduceKVPairIteratorTest.h"
#include "tests/mapreduce/common/ReservoirSamplingKVPairWriterTest.h"
#include "tests/mapreduce/common/SampleMetadataKVPairBufferTest.h"
#include "tests/mapreduce/common/TextLineFormatReaderTest.h"
#include "tests/mapreduce/common/sorting/SortingTestSuite.h"

class MapredCommonTestSuite : public CppUnit::TestSuite {
public:
  MapredCommonTestSuite() : CppUnit::TestSuite("MapReduce common objects") {
    addTest(BoundaryListTest::suite());
    addTest(DiskBackedBoundaryKeyListTest::suite());
    addTest(FilenameToStreamIDMapTest::suite());
    addTest(KVPairBufferTest::suite());
    addTest(KVPairFormatReaderTest::suite());
    addTest(KVPairWriterTest::suite());
    addTest(KeyValuePairTest::suite());
    addTest(PhaseZeroSampleMetadataTest::suite());
    addTest(ReadRequestTest::suite());
    addTest(RecordFilterTest::suite());
    addTest(ReduceKVPairIteratorTest::suite());
    addTest(ReservoirSamplingKVPairWriterTest::suite());
    addTest(SampleMetadataKVPairBufferTest::suite());
    addTest(TextLineFormatReaderTest::suite());
    addTest(new SortingTestSuite());
  }
};

#endif // MAPRED_COMMON_TEST_SUITE_H
