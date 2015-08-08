#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/functions/map/CombiningWordCountMapVerifyingWriter.h"

CombiningWordCountMapVerifyingWriter::CombiningWordCountMapVerifyingWriter()
  : pendingCount(NULL) {

}

void CombiningWordCountMapVerifyingWriter::write(KeyValuePair& kvPair) {
  std::string word(
    reinterpret_cast<const char*>(kvPair.getKey()), kvPair.getKeyLength());

  uint64_t count = *(reinterpret_cast<const uint64_t*>(kvPair.getValue()));

  counts[word].push_back(count);
}

uint8_t* CombiningWordCountMapVerifyingWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength) {
  ABORT_IF(pendingCount != NULL, "Can't setup a write while another write is "
           "already in progress");

  pendingWord.assign(reinterpret_cast<const char*>(key), keyLength);
  pendingCount = new uint8_t[maxValueLength];

  return pendingCount;
}

void CombiningWordCountMapVerifyingWriter::commitWrite(uint32_t valueLength) {
  uint64_t count = *(reinterpret_cast<uint64_t*>(pendingCount));

  counts[pendingWord].push_back(count);
  pendingWord.clear();

  delete[] pendingCount;
  pendingCount = NULL;
}

void CombiningWordCountMapVerifyingWriter::flushBuffers() {
  ABORT("Not implemented");
}

uint64_t CombiningWordCountMapVerifyingWriter::getNumBytesCallerTriedToWrite()
  const {
  ABORT("Not implemented");
  return 0;
}

uint64_t CombiningWordCountMapVerifyingWriter::getNumBytesWritten() const {
  ABORT("Not implemented");
  return 0;
}

uint64_t CombiningWordCountMapVerifyingWriter::getNumTuplesWritten() const {
  ABORT("Not implemented");
  return 0;
}
