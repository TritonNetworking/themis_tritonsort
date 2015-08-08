#include "BytesCountMapFunction.h"
#include "mapreduce/common/KeyValuePair.h"

const uint64_t BytesCountMapFunction::COUNT = 1;

BytesCountMapFunction::BytesCountMapFunction(uint64_t _numKeyBytesToCount)
  : numKeyBytesToCount(_numKeyBytesToCount) {
}

void BytesCountMapFunction::map(KeyValuePair& kvPair,
                                KVPairWriterInterface& writer) {
  KeyValuePair outputKVPair;

  outputKVPair.setKey(kvPair.getKey(), numKeyBytesToCount);
  outputKVPair.setValue(reinterpret_cast<const uint8_t*>(&COUNT),
                        sizeof(COUNT));

  writer.write(outputKVPair);
}
