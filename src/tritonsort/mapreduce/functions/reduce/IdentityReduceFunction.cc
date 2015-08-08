#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/reduce/IdentityReduceFunction.h"

void IdentityReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {
  // Emit each key/value pair in turn

  KeyValuePair kvPair;

  while (iterator.next(kvPair)) {
    writer.write(kvPair);
  }
}
