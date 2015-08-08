#include <arpa/inet.h>

#include "core/ByteOrder.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/reduce/SumValuesReduceFunction.h"

void SumValuesReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  uint64_t sum = 0;

  KeyValuePair kvPair;

  while (iterator.next(kvPair)) {
    uint32_t valueLength = kvPair.getValueLength();
    uint64_t currentCount = 0;
    if (valueLength == sizeof(uint32_t)) {
      // Values are 32 bit numbers in network byte order
      currentCount = ntohl(*(reinterpret_cast<const uint32_t*>(
                               kvPair.getValue())));
    } else if (valueLength == sizeof(uint64_t)) {
      // Values are big-endian 64 bit numbers
      currentCount = bigEndianToHost64(
        *(reinterpret_cast<const uint64_t*>(kvPair.getValue())));
    } else {
      ABORT("SumValuesReduceFunction can only handle 32 and 64 bit numbers");
    }

    sum += currentCount;
  }

  KeyValuePair outputKVPair;
  outputKVPair.setKey(key, keyLength);
  outputKVPair.setValue(reinterpret_cast<const uint8_t*>(&sum), sizeof(sum));

  writer.write(outputKVPair);
}
