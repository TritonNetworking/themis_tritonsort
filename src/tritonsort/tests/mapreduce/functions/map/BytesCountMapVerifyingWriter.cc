#include <cppunit/TestAssert.h>

#include "BytesCountMapVerifyingWriter.h"
#include "mapreduce/common/KeyValuePair.h"

BytesCountMapVerifyingWriter::BytesCountMapVerifyingWriter(
  uint32_t _expectedKeySize, uint64_t _expectedCount)
  : expectedKeySize(_expectedKeySize), expectedCount(_expectedCount) {
}

void BytesCountMapVerifyingWriter::write(KeyValuePair& kvPair) {
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected key lengths don't match.",
                               expectedKeySize, kvPair.getKeyLength());

  uint32_t expectedValueSize = sizeof(expectedCount);
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected value lengths don't match.",
                               expectedValueSize, kvPair.getValueLength());

  uint64_t count = *(reinterpret_cast<const uint64_t*>(kvPair.getValue()));
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected counts don't match.",
                               expectedCount, count);
}

void BytesCountMapVerifyingWriter::flushBuffers() {
}
