#include "PassThroughMapFunction.h"

void PassThroughMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer)  {
  writer.write(kvPair);
}
