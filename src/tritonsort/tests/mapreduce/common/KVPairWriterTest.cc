#include <boost/bind.hpp>
#include <string.h>

#include "KVPairWriterTest.h"
#include "common/SimpleMemoryAllocator.h"
#include "mapreduce/common/FastKVPairWriter.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/KVPairWriter.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PhaseZeroKVPairWriteStrategy.h"
#include "mapreduce/functions/partition/SinglePartitionMergingPartitionFunction.h"

void KVPairWriterTest::SetUp() {
  bufferFactory = NULL;
  parentWorker = NULL;
  writer = NULL;
  memoryAllocator = NULL;

  memoryAllocator = new SimpleMemoryAllocator();

  NamedObjectCollection dependencies;
  Params params;
  params.add<uint64_t>("DEFAULT_BUFFER_SIZE", 1000);
  parentWorker = dynamic_cast<KVPairWriterParentWorker*>(
    KVPairWriterParentWorker::newInstance(
      "dummy_phase_name", "dummy_stage_name", 0, params, *memoryAllocator,
      dependencies));

  EXPECT_TRUE(parentWorker != NULL);

  createNewKVPairWriter(1, NULL, NULL);
}

void KVPairWriterTest::TearDown() {
  if (bufferFactory != NULL) {
    delete bufferFactory;
    bufferFactory = NULL;
  }

  if (parentWorker != NULL) {
    delete parentWorker;
    parentWorker = NULL;
  }

  if (writer != NULL) {
    delete writer;
    writer = NULL;
  }

  if (memoryAllocator != NULL) {
    delete memoryAllocator;
    memoryAllocator = NULL;
  }
}

void KVPairWriterTest::createNewKVPairWriter(
  uint64_t numBuffers, PartitionFunctionInterface* partitionFunction,
  KVPairWriteStrategyInterface* writeStrategy) {

  if (writer != NULL) {
    delete writer;
    writer = NULL;
  }

  if (partitionFunction == NULL) {
    partitionFunction = new SinglePartitionMergingPartitionFunction();
  }

  writer = new KVPairWriter(
    numBuffers, *partitionFunction, writeStrategy,
    100, NULL,
    boost::bind(
      &KVPairWriterParentWorker::emitBufferFromWriter, parentWorker, _1, _2),
    boost::bind(
      &KVPairWriterParentWorker::getBufferForWriter, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::putBufferFromWriter, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::logSample, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::logWriteStats, parentWorker, _1, _1, _2, _2),
    true);
}

void KVPairWriterTest::createNewFastKVPairWriter(
  PartitionFunctionInterface* partitionFunction) {

  if (writer != NULL) {
    delete writer;
    writer = NULL;
  }

  if (partitionFunction == NULL) {
    partitionFunction = new SinglePartitionMergingPartitionFunction();
  }

  writer = new FastKVPairWriter(
    partitionFunction, 100,
    boost::bind(
      &KVPairWriterParentWorker::emitBufferFromWriter, parentWorker, _1, _2),
    boost::bind(
      &KVPairWriterParentWorker::getBufferForWriter, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::putBufferFromWriter, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::logSample, parentWorker, _1),
    boost::bind(
      &KVPairWriterParentWorker::logWriteStats, parentWorker, _1, _1, _2, _2),
    true, false);
}

void KVPairWriterTest::createNewTuple(uint8_t** key, uint64_t keyLength,
                                      uint8_t** value, uint64_t valueLength) {
  *key = new uint8_t[keyLength];
  *value = new uint8_t[valueLength];

  for (uint32_t i = 0; i < keyLength; i++) {
    (*key)[i] = i;
  }

  for (uint32_t i = 0; i < valueLength; i++) {
    (*value)[i] = i;
  }
}

TEST_F(KVPairWriterTest, testStandardWrite) {
  createNewKVPairWriter(1, NULL, NULL);

  uint32_t keyLength = 20;
  uint32_t valueLength = 80;
  uint8_t* key;
  uint8_t* value;

  createNewTuple(&key, keyLength, &value, valueLength);

  KeyValuePair kvPair;
  kvPair.setKey(key, keyLength);
  kvPair.setValue(value, valueLength);

  // Test write with regular writer
  writer->write(kvPair);
  writer->flushBuffers();

  validateStandardWrite(key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  // Test write with fast writer
  createNewFastKVPairWriter(NULL);

  writer->write(kvPair);
  writer->flushBuffers();

  validateStandardWrite(key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  key = NULL;
  delete[] value;
  value = NULL;
}

TEST_F(KVPairWriterTest, testLargeTuple) {
  createNewKVPairWriter(1, NULL, NULL);

  uint32_t keyLength = 500;
  uint32_t valueLength = 300;
  uint8_t* key;
  uint8_t* value;

  createNewTuple(&key, keyLength, &value, valueLength);

  KeyValuePair kvPair;
  kvPair.setKey(key, keyLength);
  kvPair.setValue(value, valueLength);

  // Test large tuple with regular writer
  writer->write(kvPair);
  writer->write(kvPair);
  writer->flushBuffers();

  validateLargeWrite(key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  // Test large tuple with fast writer
  createNewFastKVPairWriter(NULL);

  writer->write(kvPair);
  writer->write(kvPair);
  writer->flushBuffers();

  validateLargeWrite(key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  key = NULL;
  delete[] value;
  value = NULL;
}

TEST_F(KVPairWriterTest, testHugeTuple) {
  createNewKVPairWriter(1, NULL, NULL);

  uint32_t keyLength = 900;
  uint32_t valueLength = 400;
  uint8_t* key;
  uint8_t* value;

  createNewTuple(&key, keyLength, &value, valueLength);

  KeyValuePair kvPair;
  kvPair.setKey(key, keyLength);
  kvPair.setValue(value, valueLength);

  // Test huge tuple with regular writer
  writer->write(kvPair);
  writer->flushBuffers();

  const std::list<KVPairBuffer*>& emittedBuffers =
    parentWorker->getEmittedBuffers();

  EXPECT_EQ(static_cast<size_t>(1), emittedBuffers.size());

  KVPairBuffer* buffer = *emittedBuffers.begin();

  EXPECT_EQ(static_cast<uint64_t>(KeyValuePair::tupleSize(900, 400)),
            buffer->getCurrentSize());
  EXPECT_EQ(static_cast<uint64_t>(2000), buffer->getCapacity());
  EXPECT_EQ(static_cast<uint64_t>(1), buffer->getNumTuples());

  KeyValuePair emittedKVPair;
  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  checkTuple(emittedKVPair, key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  // Test huge tuple with fast writer
  createNewFastKVPairWriter(NULL);

  writer->write(kvPair);
  writer->flushBuffers();

  EXPECT_EQ(static_cast<size_t>(1), emittedBuffers.size());

  buffer = *emittedBuffers.begin();

  EXPECT_EQ(static_cast<uint64_t>(KeyValuePair::tupleSize(900, 400)),
            buffer->getCurrentSize());
  EXPECT_EQ(static_cast<uint64_t>(2000), buffer->getCapacity());
  EXPECT_EQ(static_cast<uint64_t>(1), buffer->getNumTuples());

  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  checkTuple(emittedKVPair, key, keyLength, value, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  key = NULL;
  delete[] value;
  value = NULL;
}

void KVPairWriterTest::appendTuple(uint8_t* key, uint32_t keyLength,
                                   uint32_t valueLength) {
  for (uint32_t i = 0; i < keyLength; i++) {
    key[i] = i;
  }

  uint8_t* value = writer->setupWrite(key, keyLength, valueLength);

  for (uint32_t i = 0; i < valueLength; i++) {
    value[i] = i;
  }

  writer->commitWrite(valueLength);
}

TEST_F(KVPairWriterTest, testSetupAndCommitTupleWrite) {
  createNewKVPairWriter(1, NULL, NULL);

  uint32_t keyLength = 20;
  uint32_t valueLength = 80;
  uint8_t* key = new uint8_t[keyLength];
  uint8_t* expectedValue = new uint8_t[valueLength];

  for (uint32_t i = 0; i < valueLength; i++) {
    expectedValue[i] = i;
  }

  // Test setup/commit with regular writer
  appendTuple(key, keyLength, valueLength);
  writer->flushBuffers();

  validateStandardWrite(key, keyLength, expectedValue, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  // Test setup/commit with fast writer
  createNewFastKVPairWriter(NULL);

  appendTuple(key, keyLength, valueLength);
  writer->flushBuffers();

  validateStandardWrite(key, keyLength, expectedValue, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  delete[] expectedValue;
}

TEST_F(KVPairWriterTest, testSetupAndCommitLargeTuple) {
  createNewKVPairWriter(1, NULL, NULL);

  uint32_t keyLength = 500;
  uint32_t valueLength = 300;
  uint8_t* key = new uint8_t[keyLength];
  uint8_t* expectedValue = new uint8_t[valueLength];

  for (uint32_t i = 0; i < valueLength; i++) {
    expectedValue[i] = i;
  }

  // Test setup/commit for large tuple with regular writer
  appendTuple(key, keyLength, valueLength);
  appendTuple(key, keyLength, valueLength);
  writer->flushBuffers();

  validateLargeWrite(key, keyLength, expectedValue, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  // Test setup/commit for large tuple with fast writer
  createNewFastKVPairWriter(NULL);

  appendTuple(key, keyLength, valueLength);
  appendTuple(key, keyLength, valueLength);
  writer->flushBuffers();

  validateLargeWrite(key, keyLength, expectedValue, valueLength);

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  delete[] expectedValue;
}

void KVPairWriterTest::checkTuple(KeyValuePair& kvPair, uint8_t* referenceKey,
                                  uint32_t keyLength, uint8_t* referenceValue,
                                  uint32_t valueLength) {
  EXPECT_EQ(keyLength, kvPair.getKeyLength());

  if (referenceKey != NULL) {
    EXPECT_EQ(0, memcmp(referenceKey, kvPair.getKey(), keyLength));
  }

  EXPECT_EQ(valueLength, kvPair.getValueLength());

  if (referenceValue != NULL) {
    EXPECT_EQ(0, memcmp(referenceValue, kvPair.getValue(), valueLength));
  }
}


void KVPairWriterTest::validateStandardWrite(
  uint8_t* key, uint32_t keyLength, uint8_t* value, uint32_t valueLength) {
  const std::list<KVPairBuffer*>& emittedBuffers =
    parentWorker->getEmittedBuffers();

  EXPECT_EQ(static_cast<size_t>(1), emittedBuffers.size());

  KVPairBuffer* buffer = emittedBuffers.front();
  EXPECT_EQ(
    static_cast<uint64_t>(KeyValuePair::tupleSize(keyLength, valueLength)),
    buffer->getCurrentSize());
  EXPECT_EQ(static_cast<uint64_t>(1), buffer->getNumTuples());

  KeyValuePair emittedKVPair;
  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  checkTuple(emittedKVPair, key, keyLength, value, valueLength);
}

void KVPairWriterTest::validateLargeWrite(
  uint8_t* key, uint32_t keyLength, uint8_t* value, uint32_t valueLength) {
  const std::list<KVPairBuffer*>& emittedBuffers =
    parentWorker->getEmittedBuffers();

  EXPECT_EQ(static_cast<size_t>(2), emittedBuffers.size());

  for (std::list<KVPairBuffer*>::const_iterator iter = emittedBuffers.begin();
       iter != emittedBuffers.end(); iter++) {
    KVPairBuffer* buffer = *iter;

    EXPECT_EQ(
      static_cast<uint64_t>(KeyValuePair::tupleSize(keyLength, valueLength)),
      buffer->getCurrentSize());
    EXPECT_EQ(static_cast<uint64_t>(1), buffer->getNumTuples());

    KeyValuePair emittedKVPair;

    EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));

    checkTuple(emittedKVPair, key, keyLength, value, valueLength);
  }
}

TEST_F(KVPairWriterTest, testPhaseZeroStrategy) {
  PhaseZeroKVPairWriteStrategy* phaseZeroStrategy =
    new PhaseZeroKVPairWriteStrategy();

  createNewKVPairWriter(1, NULL, phaseZeroStrategy);

  uint32_t maxKeyLength = 50;
  uint8_t* key = new uint8_t[maxKeyLength];

  appendTuple(key, 20, 80);
  appendTuple(key, 30, 60);
  appendTuple(key, 50, 70);
  appendTuple(key, 22, 37);

  writer->flushBuffers();

  const std::list<KVPairBuffer*>& emittedBuffers =
    parentWorker->getEmittedBuffers();

  EXPECT_EQ(static_cast<size_t>(1), emittedBuffers.size());

  KVPairBuffer* buffer = emittedBuffers.front();

  // Header + Key + uint64_t tuple size for each tuple
  uint64_t totalSize =
    KeyValuePair::tupleSize(20, sizeof(uint64_t)) +
    KeyValuePair::tupleSize(30, sizeof(uint64_t)) +
    KeyValuePair::tupleSize(50, sizeof(uint64_t)) +
    KeyValuePair::tupleSize(22, sizeof(uint64_t));

  EXPECT_EQ(totalSize, buffer->getCurrentSize());

  KeyValuePair emittedKVPair;
  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  uint64_t value = KeyValuePair::tupleSize(20, 80);
  uint8_t* valuePointer = reinterpret_cast<uint8_t*>(&value);
  checkTuple(emittedKVPair, key, 20, valuePointer, sizeof(valuePointer));

  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  value = KeyValuePair::tupleSize(30, 60);
  checkTuple(emittedKVPair, key, 30, valuePointer, sizeof(valuePointer));

  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  value = KeyValuePair::tupleSize(50, 70);
  checkTuple(emittedKVPair, key, 50, valuePointer, sizeof(valuePointer));

  EXPECT_EQ(true, buffer->getNextKVPair(emittedKVPair));
  value = KeyValuePair::tupleSize(22, 37);
  checkTuple(emittedKVPair, key, 22, valuePointer, sizeof(valuePointer));

  EXPECT_TRUE(!buffer->getNextKVPair(emittedKVPair));

  parentWorker->returnEmittedBuffersToPool();

  delete[] key;
  key = NULL;
}
