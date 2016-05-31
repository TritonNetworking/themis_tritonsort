#include <stdint.h>

#include "BytesCountMapFunctionTest.h"
#include "BytesCountMapVerifyingWriter.h"
#include "common/DummyWorker.h"
#include "common/SimpleMemoryAllocator.h"
#include "core/Params.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/functions/map/BytesCountMapFunction.h"

BytesCountMapFunctionTest::BytesCountMapFunctionTest() {
  keyBytes = new uint8_t[keyLength];
  valueBytes = new uint8_t[valueLength];
}

BytesCountMapFunctionTest::~BytesCountMapFunctionTest() {
  delete[] keyBytes;
  delete[] valueBytes;
}

TEST_F(BytesCountMapFunctionTest, testProperCountStored) {
  Params params;

  SimpleMemoryAllocator memoryAllocator;
  DummyWorker dummyWorker(0, "test");

  uint64_t callerID = memoryAllocator.registerCaller(dummyWorker);

  KVPairBuffer buffer(memoryAllocator, callerID, 1000,0);

  KeyValuePair kvPair;
  kvPair.setKey(keyBytes, keyLength);
  kvPair.setValue(valueBytes, valueLength);

  for (uint64_t i = 0; i < keyLength; i++) {
    keyBytes[i] = i;
  }

  for (uint64_t i = 0; i < valueLength; i++) {
    valueBytes[i] = i;
  }

  BytesCountMapFunction mapFunction(3);

  BytesCountMapVerifyingWriter writer(3, 1);

  mapFunction.map(kvPair, writer);
}
