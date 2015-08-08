#ifndef THEMIS_WEX_LINK_EXTRACTOR_MAP_VERIFYING_WRITER_H
#define THEMIS_WEX_LINK_EXTRACTOR_MAP_VERIFYING_WRITER_H

#include <set>
#include <stdint.h>
#include <string>

#include "mapreduce/common/KVPairWriterInterface.h"

class WEXLinkExtractorMapFunctionVerifyingWriter
  : public KVPairWriterInterface {
public:
  void write(KeyValuePair& kvPair);
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength,
    uint32_t maxValueLength);
  void commitWrite(uint32_t valueLength);
  void flushBuffers();
  uint64_t getNumBytesCallerTriedToWrite() const;
  uint64_t getNumBytesWritten() const;
  uint64_t getNumTuplesWritten() const;

  std::set<std::string> keys;
  std::set<std::string> internalLinks;
};

#endif // THEMIS_WEX_LINK_EXTRACTOR_MAP_VERIFYING_WRITER_H
