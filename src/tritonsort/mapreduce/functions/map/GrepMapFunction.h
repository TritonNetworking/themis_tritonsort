#ifndef MAPRED_GREP_MAP_FUNCTION_H
#define MAPRED_GREP_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

/**
   A map function that emits a certain percentage of the tuples passed to it to
   simulate what would happen in a real grep
 */
class GrepMapFunction : public MapFunction {
public:
  /// Constructor
  /**
     Grep is passed a "selectivity" in [0, 1] that describes what percentage of
     tuples will be emitted by the grep. This number is scaled to [0, 256) to
     generate the "threshold byte value" (see GrepMapFunction::map).

     \param grepSelectivity a fraction in [0, 1] that describes what percentage
     of tuples will be emitted by the grep
   */
  GrepMapFunction(double grepSelectivity);

  /**
     If the first byte of the tuple's value is less than the threshold byte
     value, it is emitted. Otherwise, it is swallowed by the grep and nothing
     is emitted..

     \sa MapFunction::map
   */
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  /// The "threshold" byte value
  uint8_t thresholdByteValue;
};

#endif // MAPRED_GREP_MAP_FUNCTION_H
