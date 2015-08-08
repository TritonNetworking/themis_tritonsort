#ifndef THEMIS_STRING_LIST_VERIFYING_WRITER_H
#define THEMIS_STRING_LIST_VERIFYING_WRITER_H

#include <list>
#include <string>

#include "mapreduce/common/KVPairWriterInterface.h"

/**
   A StringListVerifyingWriter interprets keys and values as strings and stores
   them in lists, which can be accessed directly in tests.
 */
class StringListVerifyingWriter : public KVPairWriterInterface {
public:
  StringListVerifyingWriter();
  virtual ~StringListVerifyingWriter() {}

  void write(KeyValuePair& kvPair);
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength);
  void commitWrite(uint32_t valueLength);
  void flushBuffers();
  uint64_t getNumBytesCallerTriedToWrite() const;
  uint64_t getNumBytesWritten() const;
  uint64_t getNumTuplesWritten() const;

  std::list<std::string> keys;
  std::list<std::string> values;
  uint8_t* pendingValue;
};

#endif // THEMIS_STRING_LIST_VERIFYING_WRITER_H
