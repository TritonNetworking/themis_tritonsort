#ifndef THEMIS_SORT_STRATEGY_TEST_SUITE_H
#define THEMIS_SORT_STRATEGY_TEST_SUITE_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>
#include <stdint.h>

#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/SortStrategyInterface.h"

class SortStrategyTestSuite : public CppUnit::TestFixture {
public:
  void setUp();
  void tearDown();

protected:
  void testUniformSizeRecords(
    SortStrategyInterface& sortStrategy,
    uint32_t numRecords, uint32_t keyLength, uint32_t valueLength,
    bool secondaryKeys);

  void testVariableSizeRecords(
    SortStrategyInterface& sortStrategy, uint32_t numRecords,
    bool secondaryKeys);

  void setupUniformRecordSizeBuffer(
    uint32_t numRecords, uint32_t keyLength, uint32_t valueLength);

  void setupRandomlySizedRecordsBuffer(uint64_t numRecords);

  void setupOutputBuffer();

  void setupScratchSpace(SortStrategyInterface& strategy);

  void assertSorted(KVPairBuffer* buffer, bool checkSecondaryKeys);

  KVPairBuffer* inputBuffer;
  KVPairBuffer* outputBuffer;
  uint8_t* inputBufferMemory;
  uint8_t* outputBufferMemory;
  uint8_t* scratchMemory;
};


#endif // THEMIS_SORT_STRATEGY_TEST_SUITE_H
