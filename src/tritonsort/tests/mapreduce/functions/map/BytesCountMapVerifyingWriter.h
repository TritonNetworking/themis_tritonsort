#ifndef BYTES_COUNT_MAP_VERIFYING_WRITER_H
#define BYTES_COUNT_MAP_VERIFYING_WRITER_H

#include <stdint.h>

#include "core/TritonSortAssert.h"
#include "mapreduce/common/KVPairWriterInterface.h"
#include "mapreduce/common/KeyValuePair.h"

class BytesCountMapVerifyingWriter : public KVPairWriterInterface {
public:
  BytesCountMapVerifyingWriter(uint32_t expectedKeySize,
                               uint64_t expectedCount);
  void write(KeyValuePair& kvPair);
  uint8_t* setupWrite(const uint8_t* key, uint32_t keyLength,
                      uint32_t maxValueLength) {
    ABORT("Not implemented");
    return NULL;
  }

  void commitWrite(uint32_t valueLength) {
    ABORT("Not implemented");
  }

  void flushBuffers();

  uint64_t getNumBytesCallerTriedToWrite() const {
    ABORT("Not implemented");
    return 0;
  }

  uint64_t getNumBytesWritten() const {
    ABORT("Not implemented");
    return 0;
  }

  uint64_t getNumTuplesWritten() const {
    ABORT("Not implemented");
    return 0;
  }
private:
  const uint32_t expectedKeySize;
  const uint64_t expectedCount;
};

#endif // BYTES_COUNT_MAP_VERIFYING_WRITER_H
