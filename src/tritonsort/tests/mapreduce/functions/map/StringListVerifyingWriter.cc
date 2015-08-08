#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/functions/map/StringListVerifyingWriter.h"

StringListVerifyingWriter::StringListVerifyingWriter()
  : pendingValue(NULL) {

}

void StringListVerifyingWriter::write(KeyValuePair& kvPair) {
  keys.push_back(
    std::string(
      reinterpret_cast<const char*>(kvPair.getKey()), kvPair.getKeyLength()));
  values.push_back(
    std::string(
      reinterpret_cast<const char*>(
        kvPair.getValue()), kvPair.getValueLength()));
}

uint8_t* StringListVerifyingWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength,  uint32_t maxValueLength) {

  ABORT_IF(pendingValue != NULL, "Can't setup a write if another write is "
         "already in progress");

  keys.push_back(std::string(reinterpret_cast<const char*>(key), keyLength));

  pendingValue = new uint8_t[maxValueLength];
  return pendingValue;
}

void StringListVerifyingWriter::commitWrite(uint32_t valueLength) {
  values.push_back(
    std::string(reinterpret_cast<char*>(pendingValue), valueLength));
  delete[] pendingValue;
  pendingValue = NULL;
}

void StringListVerifyingWriter::flushBuffers() {
  ABORT("Not implemented.");
}

uint64_t StringListVerifyingWriter::getNumBytesCallerTriedToWrite() const {
  ABORT("Not implemented.");
  return 0;
}

uint64_t StringListVerifyingWriter::getNumBytesWritten() const {
  ABORT("Not implemented.");
  return 0;
}

uint64_t StringListVerifyingWriter::getNumTuplesWritten() const {
  ABORT("Not implemented.");
  return 0;
}
