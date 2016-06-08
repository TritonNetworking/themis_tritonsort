#include "gtest.h"

#include "BytesCountMapVerifyingWriter.h"
#include "mapreduce/common/KeyValuePair.h"

BytesCountMapVerifyingWriter::BytesCountMapVerifyingWriter(
  uint32_t _expectedKeySize, uint64_t _expectedCount)
  : expectedKeySize(_expectedKeySize), expectedCount(_expectedCount) {
}

void BytesCountMapVerifyingWriter::write(KeyValuePair& kvPair) {
  EXPECT_EQ(expectedKeySize, kvPair.getKeyLength());

  uint32_t expectedValueSize = sizeof(expectedCount);
  EXPECT_EQ(expectedValueSize, kvPair.getValueLength());

  uint64_t count = *(reinterpret_cast<const uint64_t*>(kvPair.getValue()));
  EXPECT_EQ(expectedCount, count);
}

void BytesCountMapVerifyingWriter::flushBuffers() {
}
