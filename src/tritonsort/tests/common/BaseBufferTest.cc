#include <string.h>

#include "common/buffers/BaseBuffer.h"
#include "core/TritonSortAssert.h"
#include "tests/common/BaseBufferTest.h"

void BaseBufferTest::testAlignment() {
  uint64_t capacity = 123456789;
  uint64_t alignment = 9876543;
  BaseBuffer buffer(capacity, alignment);
  // Interpret raw buffer as an integer and verify it is aligned.
  uint64_t address = reinterpret_cast<uint64_t>(buffer.getRawBuffer());
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Buffer not aligned.", static_cast<uint64_t>(0), address % alignment);
}

void BaseBufferTest::testSeek() {
  uint64_t capacity = 10000;
  uint64_t alignment = 0;
  BaseBuffer buffer(capacity, alignment);
  // Interpret raw buffer as integer so we can verify seek distance.
  uint64_t startAddress = reinterpret_cast<uint64_t>(buffer.getRawBuffer());
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(0), buffer.getCurrentSize());
  CPPUNIT_ASSERT_EQUAL(capacity, buffer.getCapacity());

  // Try to seek forward 500 bytes.
  CPPUNIT_ASSERT_NO_THROW(buffer.seekForward(500));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(0), buffer.getCurrentSize());
  CPPUNIT_ASSERT_EQUAL(capacity - 500, buffer.getCapacity());
  CPPUNIT_ASSERT_EQUAL(
    startAddress + 500, reinterpret_cast<uint64_t>(buffer.getRawBuffer()));

  // Try to seek backward 200 bytes.
  CPPUNIT_ASSERT_NO_THROW(buffer.seekBackward(200));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(200), buffer.getCurrentSize());
  CPPUNIT_ASSERT_EQUAL(capacity - 300, buffer.getCapacity());
  CPPUNIT_ASSERT_EQUAL(
    startAddress + 300, reinterpret_cast<uint64_t>(buffer.getRawBuffer()));

  // Try to seek forward 100 bytes.
  CPPUNIT_ASSERT_NO_THROW(buffer.seekForward(100));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(100), buffer.getCurrentSize());
  CPPUNIT_ASSERT_EQUAL(capacity - 400, buffer.getCapacity());
  CPPUNIT_ASSERT_EQUAL(
    startAddress + 400, reinterpret_cast<uint64_t>(buffer.getRawBuffer()));

  // Seeking forward 9601 bytes should fail since we will overshoot the buffer.
  CPPUNIT_ASSERT_THROW(buffer.seekForward(9601), AssertionFailedException);

  // Seeking backward 401 bytes should fail since we will seek past the start
  // address.
  CPPUNIT_ASSERT_THROW(buffer.seekBackward(401), AssertionFailedException);
}

void BaseBufferTest::testAppend() {
  uint64_t capacity = 500;
  uint64_t alignment = 0;
  BaseBuffer buffer(capacity, alignment);

  // Zero the buffer.
  uint8_t* rawBuffer = const_cast<uint8_t*>(buffer.getRawBuffer());
  memset(rawBuffer, 0, capacity);

  uint8_t* sourceBytes = new uint8_t[400];
  for (uint64_t i = 0; i < 200; i++) {
    sourceBytes[i] = i;
  }

  // Verify that a 200 byte append correctly copies data.
  CPPUNIT_ASSERT_NO_THROW(buffer.append(sourceBytes, 200));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(200), buffer.getCurrentSize());

  for (uint64_t i = 0; i < 200; i++) {
    CPPUNIT_ASSERT_EQUAL(sourceBytes[i], rawBuffer[i]);
  }

  // Verify that a 400 byte append, which overflows the buffer, fails an assert.
  CPPUNIT_ASSERT_THROW(
    buffer.append(sourceBytes, 400), AssertionFailedException);
}

void BaseBufferTest::testSetupCommitAppend() {
  uint64_t capacity = 7777;
  uint64_t alignment = 0;
  BaseBuffer buffer(capacity, alignment);

  // Zero the buffer.
  uint8_t* rawBuffer = const_cast<uint8_t*>(buffer.getRawBuffer());
  memset(rawBuffer, 0, capacity);

  // Verify that setupAppend fails if the append length is too long.
  CPPUNIT_ASSERT_THROW(buffer.setupAppend(8000), AssertionFailedException);

  // Verify that calling setupAppend twice without aborting fails.
  uint8_t* appendPtr = NULL;
  CPPUNIT_ASSERT_NO_THROW(
    appendPtr = const_cast<uint8_t*>(buffer.setupAppend(6000)));
  CPPUNIT_ASSERT_THROW(buffer.setupAppend(6000), AssertionFailedException);

  // Verify that you can abort and then call setup append again.
  CPPUNIT_ASSERT_NO_THROW(buffer.abortAppend(appendPtr));
  CPPUNIT_ASSERT_NO_THROW(
    appendPtr = const_cast<uint8_t*>(buffer.setupAppend(2000)));

  // Write to the pointer given.
  for (uint64_t i = 0; i < 1000; i++) {
    appendPtr[i] = i;
  }

  // Verify that committing too much fails.
  CPPUNIT_ASSERT_THROW(
    buffer.commitAppend(appendPtr, 3000), AssertionFailedException);

  // Verify that committing the wrong pointer fails.
  CPPUNIT_ASSERT_THROW(
    buffer.commitAppend(appendPtr + 5, 1000), AssertionFailedException);

  // Verify that committing correctly updates the buffer appropriately.
  CPPUNIT_ASSERT_NO_THROW(buffer.commitAppend(appendPtr, 1000));
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(1000), buffer.getCurrentSize());

  for (uint64_t i = 0; i < 1000; i++) {
    CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>(i), rawBuffer[i]);
  }
}
