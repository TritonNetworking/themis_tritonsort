#include <stdint.h>
#include <string.h>

#include "KeyValuePairTest.h"
#include "mapreduce/common/KeyValuePair.h"

void KeyValuePairTest::setUp() {
  // Fill the raw buffer with the following tuples:
  // 10 90
  // 0 50
  // 10 0
  // 0 0

  // Fill a source buffer with the bytes 100, 99, ... , 1
  uint8_t sourceBuffer[100];
  for (uint8_t i = 0; i < 100; ++i) {
    sourceBuffer[i] = 100 - i;
  }

  firstByte = 100;

  // Append the three tuples to the raw buffer.
  uint32_t keyLength = 10;
  uint32_t valueLength = 90;
  uint8_t* buffer = rawBuffer;

  KeyValuePair kvPair;
  kvPair.setKey(sourceBuffer, keyLength);
  kvPair.setValue(sourceBuffer, valueLength);
  kvPair.serialize(buffer);
  buffer += kvPair.getWriteSize();

  // Mark the end of the first tuple.
  endOfFirstTuple = buffer;

  keyLength = 0;
  valueLength = 50;

  kvPair.setKey(sourceBuffer, keyLength);
  kvPair.setValue(sourceBuffer, valueLength);
  kvPair.serialize(buffer);
  buffer += kvPair.getWriteSize();

  keyLength = 10;
  valueLength = 0;

  kvPair.setKey(sourceBuffer, keyLength);
  kvPair.setValue(sourceBuffer, valueLength);
  kvPair.serialize(buffer);
  buffer += kvPair.getWriteSize();

  keyLength = 0;
  valueLength = 0;

  kvPair.setKey(sourceBuffer, keyLength);
  kvPair.setValue(sourceBuffer, valueLength);
  kvPair.serialize(buffer);
  buffer += kvPair.getWriteSize();

  // Mark the end of the buffer.
  endOfBuffer = buffer;
}


void KeyValuePairTest::testKeyLength() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect key length for tuple 1",
    static_cast<uint32_t>(10),
    KeyValuePair::keyLength(rawBuffer));
}

void KeyValuePairTest::testValueLength() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect value length for tuple 1",
    static_cast<uint32_t>(90),
    KeyValuePair::valueLength(rawBuffer));
}

void KeyValuePairTest::testTupleSize() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect tuple size for tuple 1",
    static_cast<uint64_t>(KeyValuePair::tupleSize(10, 90)),
    KeyValuePair::tupleSize(rawBuffer));
}

void KeyValuePairTest::testNextTuple() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "nextTuple() should point to the end of tuple 1",
    endOfFirstTuple, KeyValuePair::nextTuple(rawBuffer));
}

void KeyValuePairTest::testKey() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Incorrect first byte of tuple 1 key",
                               firstByte, *(KeyValuePair::key(rawBuffer)));
}

void KeyValuePairTest::testValue() {
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Incorrect first byte of tuple 1 value",
                               firstByte, *(KeyValuePair::value(rawBuffer)));
}

void KeyValuePairTest::testMultipleTuples() {
  // The other tests check the first tuple, so skip to the second.
  uint8_t* buffer = endOfFirstTuple;
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect key length for tuple 2",
    static_cast<uint32_t>(0),
    KeyValuePair::keyLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect value length for tuple 2",
    static_cast<uint32_t>(50),
    KeyValuePair::valueLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect tuple size for tuple 2",
    static_cast<uint64_t>(KeyValuePair::tupleSize(0, 50)),
    KeyValuePair::tupleSize(buffer));
  // No key.
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Incorrect first byte of tuple 2 value",
                               firstByte, *(KeyValuePair::value(buffer)));
  uint8_t* nextTuple = KeyValuePair::nextTuple(buffer);
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Next tuple (3) isn't at the right offset",
    static_cast<uint64_t>(KeyValuePair::tupleSize(buffer)),
    reinterpret_cast<uint64_t>(nextTuple) -
    reinterpret_cast<uint64_t>(buffer));
  buffer = nextTuple;

  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect key length for tuple 3",
    static_cast<uint32_t>(10),
    KeyValuePair::keyLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect value length for tuple 3",
    static_cast<uint32_t>(0),
    KeyValuePair::valueLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect tuple size for tuple 3",
    static_cast<uint64_t>(KeyValuePair::tupleSize(10, 0)),
    KeyValuePair::tupleSize(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect first byte of tuple 3 key",
    firstByte, *(KeyValuePair::key(buffer)));
  // No value.
  nextTuple = KeyValuePair::nextTuple(buffer);
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Next tuple (4) isn't at the right offset",
    static_cast<uint64_t>(KeyValuePair::tupleSize(buffer)),
    reinterpret_cast<uint64_t>(nextTuple) -
    reinterpret_cast<uint64_t>(buffer));
  buffer = nextTuple;

  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect key length for tuple 4",
    static_cast<uint32_t>(0),
    KeyValuePair::keyLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect value length for tuple 4",
    static_cast<uint32_t>(0),
    KeyValuePair::valueLength(buffer));
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Incorrect tuple size for tuple 4",
    static_cast<uint64_t>(KeyValuePair::tupleSize(0, 0)),
    KeyValuePair::tupleSize(buffer));
  // No key or value.
  nextTuple = KeyValuePair::nextTuple(buffer);
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Last tuple end position is wrong.",
                               endOfBuffer, nextTuple);
}

void KeyValuePairTest::testSerialize() {
  KeyValuePair kvPair1;

  uint8_t* keyBlob = makeSequentialBlob(10);
  uint8_t* valueBlob = makeSequentialBlob(90);

  kvPair1.setKey(keyBlob, 10);
  kvPair1.setValue(valueBlob, 90);

  uint8_t* serializedPair1 = new uint8_t[kvPair1.getWriteSize()];
  kvPair1.serialize(serializedPair1);

  KeyValuePair kvPair2;
  kvPair2.deserialize(serializedPair1);

  CPPUNIT_ASSERT(kvPairsEqual(kvPair1, kvPair2));

  delete[] keyBlob;
  delete[] valueBlob;
  delete[] serializedPair1;
}

void KeyValuePairTest::testEqualsNoFields() {
  KeyValuePair p1;
  KeyValuePair p2;

  CPPUNIT_ASSERT(kvPairsEqual(p1, p2));
}

void KeyValuePairTest::testEqualsDifferentKeyLengths() {
  KeyValuePair p1;
  KeyValuePair p2;

  uint8_t* blob1 = makeSequentialBlob(7);
  uint8_t* blob2 = makeSequentialBlob(10);

  p1.setKey(blob1, 7);
  p2.setKey(blob2, 10);

  CPPUNIT_ASSERT(!kvPairsEqual(p1, p2));

  p2.setKey(blob1, 7);

  CPPUNIT_ASSERT(kvPairsEqual(p1, p2));

  delete[] blob1;
  delete[] blob2;
}

void KeyValuePairTest::testEqualsDifferentValueLengths() {
  KeyValuePair p1;
  KeyValuePair p2;

  uint8_t* blob1 = makeSequentialBlob(7);
  uint8_t* blob2 = makeSequentialBlob(10);

  p1.setValue(blob1, 7);
  p2.setValue(blob2, 10);

  CPPUNIT_ASSERT(!kvPairsEqual(p1, p2));

  p2.setValue(blob1, 7);

  CPPUNIT_ASSERT(kvPairsEqual(p1, p2));

  delete[] blob1;
  delete[] blob2;
}

void KeyValuePairTest::testEqualsDifferentKeys() {
  KeyValuePair p1;
  KeyValuePair p2;

  uint8_t* blob1 = makeSequentialBlob(7);
  uint8_t* blob2 = makeSequentialBlob(7);
  blob1[3] = 42;

  p1.setKey(blob1, 7);
  p2.setKey(blob2, 7);

  CPPUNIT_ASSERT(!kvPairsEqual(p1, p2));

  p1.setKey(blob2, 7);

  CPPUNIT_ASSERT(kvPairsEqual(p1, p2));

  delete[] blob1;
  delete[] blob2;
}

void KeyValuePairTest::testEqualsDifferentValues() {
  KeyValuePair p1;
  KeyValuePair p2;

  uint8_t* blob1 = makeSequentialBlob(7);
  uint8_t* blob2 = makeSequentialBlob(7);
  blob1[3] = 42;

  p1.setValue(blob1, 7);
  p2.setValue(blob2, 7);

  CPPUNIT_ASSERT(!kvPairsEqual(p1, p2));

  p1.setValue(blob2, 7);

  CPPUNIT_ASSERT(kvPairsEqual(p1, p2));

  delete[] blob1;
  delete[] blob2;
}

uint8_t* KeyValuePairTest::makeSequentialBlob(uint64_t length) {
  uint8_t* blob = new uint8_t[length];

  for (uint64_t i = 0; i < length; i++) {
    blob[i] = i;
  }

  return blob;
}

bool KeyValuePairTest::kvPairsEqual(
  const KeyValuePair& pair1, const KeyValuePair& pair2) {
  // Key lengths should be equal.
  if (pair1.getKeyLength() != pair2.getKeyLength()) {
    return false;
  }

  // Value lengths should be equal.
  if (pair1.getValueLength() != pair2.getValueLength()) {
    return false;
  }

  // Keys should be either both NULL or both non-NULL.
  if ((pair1.getKey() == NULL && pair2.getKey() != NULL) ||
      (pair1.getKey() != NULL && pair2.getKey() == NULL)) {
    return false;
  }

  // Non-NULL keys should be equal.
  if (pair1.getKey() != NULL &&
      memcmp(pair1.getKey(), pair2.getKey(), pair1.getKeyLength()) != 0) {
    return false;
  }

  // Values should be either both NULL or both non-NULL.
  if ((pair1.getValue() == NULL && pair2.getValue() != NULL) ||
      (pair1.getValue() != NULL && pair2.getValue() == NULL)) {
    return false;
  }

  // Non-NULL values should be equal.
  if (pair1.getValue() != NULL &&
      memcmp(pair1.getValue(), pair2.getValue(), pair1.getValueLength()) != 0) {
    return false;
  }

  return true;
}
