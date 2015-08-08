#ifndef MAPRED_NOP_TUPLE_SIZE_LOGGING_STRATEGY_H
#define MAPRED_NOP_TUPLE_SIZE_LOGGING_STRATEGY_H

#include "mapreduce/common/TupleSizeLoggingStrategyInterface.h"

/**
   A tuple size logging strategy that does nothing. Useful for testing.
 */
class NopTupleSizeLoggingStrategy : public TupleSizeLoggingStrategyInterface {
public:
  /// Constructor
  NopTupleSizeLoggingStrategy();

  /// Doesn't register any stats with the logger
  void registerStats(StatLogger& logger);

  /// Doesn't log anything relating to the key/value pair
  void logTuple(StatLogger& logger, KeyValuePair& kvPair);
};

#endif // MAPRED_NOP_TUPLE_SIZE_LOGGING_STRATEGY_H
