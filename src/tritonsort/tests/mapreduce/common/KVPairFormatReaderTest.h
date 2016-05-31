#ifndef MAPRED_KV_PAIR_FORMAT_READER_TEST_H
#define MAPRED_KV_PAIR_FORMAT_READER_TEST_H

#include <list>
#include <set>

#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "tests/mapreduce/common/MemoryAllocatingTestFixture.h"
#include "tests/themis_core/MockWorkerTracker.h"

class BaseBuffer;
class ByteStreamBuffer;
class ByteStreamConverter;
class KVPairBuffer;

class KVPairFormatReaderTest : public MemoryAllocatingTestFixture {
public:
  KVPairFormatReaderTest();
  void SetUp();
  void TearDown();

protected:
  void appendTuple(BaseBuffer* buffer, uint32_t keyLength,
                   uint32_t valueLength);
  void verifyBufferContainsTuple(uint8_t* buffer, uint32_t keyLength,
                                 uint32_t valueLength);
  void runInputsAndVerifyOutputs(
    std::list<ByteStreamBuffer*>& inputs,
    const std::list<uint64_t>& expectedBufferSizes,
    const std::list<uint32_t>& keySizes, const std::list<uint32_t>& valueSizes);

  // Tuple data will be sourced from this byte array.
  uint8_t rawBuffer[250];

  const std::string filename;
  std::set<uint64_t> jobIDs;

  // Dummy params.
  Params params;

  ByteStreamConverter* converter;
  MockWorkerTracker downstreamTracker;

  FilenameToStreamIDMap filenameToStreamIDMap;

  ByteStreamBuffer* inputBuffer;
  ByteStreamBuffer* streamClosedBuffer;
};

#endif // MAPRED_KV_PAIR_FORMAT_READER_TEST_H
