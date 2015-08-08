#include "core/ByteOrder.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/functions/reduce/WordCountReduceFunction.h"

void WordCountReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength,
  KVPairIterator& iterator, KVPairWriterInterface& writer) {

  const uint8_t* word = NULL;
  uint32_t wordLength = 0;

  uint64_t count = 0;

  KeyValuePair currentKVPair;

  while (iterator.next(currentKVPair)) {
    if (word == NULL) {
      // The key of this record is the hash of the word. The value is the count
      // (a big-endian uint64_t), followed by the word itself

      word = currentKVPair.getValue() + sizeof(uint64_t);
      wordLength = currentKVPair.getValueLength() - sizeof(uint64_t);
    }

    count += bigEndianToHost64(
      *(reinterpret_cast<const uint64_t*>(currentKVPair.getValue())));
  }

  KeyValuePair kvPair;
  kvPair.setKey(word, wordLength);
  kvPair.setValue(reinterpret_cast<uint8_t*>(&count), sizeof(count));

  writer.write(kvPair);
}
