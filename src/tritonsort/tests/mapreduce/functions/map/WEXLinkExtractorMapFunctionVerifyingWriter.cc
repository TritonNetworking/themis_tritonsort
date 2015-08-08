#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/functions/map/WEXLinkExtractorMapFunctionVerifyingWriter.h"

void WEXLinkExtractorMapFunctionVerifyingWriter::write(KeyValuePair& kvPair) {
  keys.insert(
    std::string(
      reinterpret_cast<const char*>(kvPair.getKey()),
      kvPair.getKeyLength()));
  internalLinks.insert(
    std::string(
      reinterpret_cast<const char*>(kvPair.getValue()),
      kvPair.getValueLength()));
}

uint8_t* WEXLinkExtractorMapFunctionVerifyingWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength,  uint32_t maxValueLength) {
  ABORT("Not implemented.");
  return NULL;
}

void WEXLinkExtractorMapFunctionVerifyingWriter::commitWrite(
  uint32_t valueLength) {
  ABORT("Not implemented.");
}

void WEXLinkExtractorMapFunctionVerifyingWriter::flushBuffers() {
  ABORT("Not implemented.");
}

uint64_t
WEXLinkExtractorMapFunctionVerifyingWriter::getNumBytesCallerTriedToWrite()
  const {
  ABORT("Not implemented.");
  return 0;
}

uint64_t
WEXLinkExtractorMapFunctionVerifyingWriter::getNumBytesWritten() const {
  ABORT("Not implemented.");
  return 0;
}

uint64_t
WEXLinkExtractorMapFunctionVerifyingWriter::getNumTuplesWritten() const {
  ABORT("Not implemented.");
  return 0;
}
