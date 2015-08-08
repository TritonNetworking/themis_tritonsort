#ifndef THEMIS_COMBINING_WORD_COUNT_MAP_VERIFYING_WRITER_H
#define THEMIS_COMBINING_WORD_COUNT_MAP_VERIFYING_WRITER_H

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "mapreduce/common/KVPairWriterInterface.h"

class CombiningWordCountMapVerifyingWriter : public KVPairWriterInterface {
public:
  CombiningWordCountMapVerifyingWriter();
  virtual ~CombiningWordCountMapVerifyingWriter() {}

  void write(KeyValuePair& kvPair);
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength);
  void commitWrite(uint32_t valueLength);
  void flushBuffers();
  uint64_t getNumBytesCallerTriedToWrite() const;
  uint64_t getNumBytesWritten() const;
  uint64_t getNumTuplesWritten() const;

  typedef std::map<std::string, std::list<uint64_t> > CountMap;
  CountMap counts;

  std::string pendingWord;
  uint8_t* pendingCount;
};

#endif // THEMIS_COMBINING_WORD_COUNT_MAP_VERIFYING_WRITER_H
