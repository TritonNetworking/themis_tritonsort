#include <limits>

#include "GrepMapFunction.h"
#include "core/TritonSortAssert.h"

GrepMapFunction::GrepMapFunction(double grepSelectivity) {
  TRITONSORT_ASSERT(grepSelectivity >= 0.0 && grepSelectivity <= 1.0, "grep selectivity "
         "must be between 0.0 and 1.0 inclusive");

  uint8_t maxByteValue = std::numeric_limits<uint8_t>::max();

  thresholdByteValue = grepSelectivity * maxByteValue;
}

void GrepMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {
  uint8_t firstValueByte = *kvPair.getValue();

  if (firstValueByte <= thresholdByteValue) {
    writer.write(kvPair);
  }
}
