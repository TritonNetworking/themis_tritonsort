#ifndef MAPRED_KVPAIR_WRITER_TEST_H
#define MAPRED_KVPAIR_WRITER_TEST_H

#include "googletest.h"

#include "KVPairWriterParentWorker.h"

class KVPairBufferFactory;
class KVPairWriteStrategyInterface;
class KVPairWriterInterface;
class PartitionFunctionInterface;
class TupleSizeLoggingStrategyInterface;
class MemoryAllocatorInterface;

class KVPairWriterTest : public ::testing::Test {
public:
  void SetUp();
  void TearDown();

protected:
  MemoryAllocatorInterface* memoryAllocator;
  KVPairBufferFactory* bufferFactory;
  KVPairWriterParentWorker* parentWorker;
  KVPairWriterInterface* writer;
  TupleSizeLoggingStrategyInterface* tupleSizeLoggingStrategy;


  void createNewKVPairWriter(
    uint64_t numBuffers, PartitionFunctionInterface* partitionFunction,
    KVPairWriteStrategyInterface* writeStrategy);

  void createNewFastKVPairWriter(PartitionFunctionInterface* partitionFunction);

  void createNewTuple(uint8_t** key, uint64_t keyLength,
                      uint8_t** value, uint64_t valueLength);

  void validateStandardWrite(uint8_t* key, uint32_t keyLength, uint8_t* value,
                             uint32_t valueLength);
  void validateLargeWrite(uint8_t* key, uint32_t keyLength, uint8_t* value,
                          uint32_t valueLength);

  void appendTuple(uint8_t* key, uint32_t keyLength, uint32_t valueLength);
  void checkTuple(KeyValuePair& kvPair, uint8_t* referenceKey,
                  uint32_t keyLength, uint8_t* referenceValue,
                  uint32_t valueLength);
};


#endif // MAPRED_KVPAIR_WRITER_TEST_H
