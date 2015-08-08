#include "ZeroKeyMapFunction.h"

void ZeroKeyMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer)  {
  kvPair.setKey(NULL, 0);
  writer.write(kvPair);
}
