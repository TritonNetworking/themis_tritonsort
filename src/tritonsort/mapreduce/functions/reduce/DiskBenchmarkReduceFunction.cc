#include <arpa/inet.h>

#include "DiskBenchmarkReduceFunction.h"
#include "mapreduce/common/KeyValuePair.h"

void DiskBenchmarkReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength,
  KVPairIterator& iterator, KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  while (iterator.next(kvPair)) {
    const uint8_t* value = kvPair.getValue();
    uint32_t valueLength = kvPair.getValueLength();

    const uint32_t writeRate = ntohl(
      *(reinterpret_cast<const uint32_t*>(value)));

    const uint8_t* diskPath = value + sizeof(writeRate);
    uint32_t diskPathLength = valueLength - sizeof(writeRate);

    uint8_t* outputValuePtr = writer.setupWrite(
      diskPath, diskPathLength, sizeof(writeRate));
    memcpy(outputValuePtr, &writeRate, sizeof(writeRate));
    writer.commitWrite(sizeof(writeRate));
  }
}
