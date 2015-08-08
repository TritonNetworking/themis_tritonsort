#ifndef MAPRED_TUPLE_SIZE_LOGGING_STRATEGY_INTERFACE_H
#define MAPRED_TUPLE_SIZE_LOGGING_STRATEGY_INTERFACE_H

#include "core/StatLogger.h"
#include "mapreduce/common/KeyValuePair.h"

/**
   Interface that any strategy for logging tuple sizes must implement
 */
class TupleSizeLoggingStrategyInterface {
public:
  /// Destructor
  virtual ~TupleSizeLoggingStrategyInterface() {}

  /// Register appropriate stats with a StatLogger
  /**
     This method should register any statistics that need registering with the
     provided StatLogger.

     \param logger the StatLogger with which to register
   */
  virtual void registerStats(StatLogger& logger) = 0;

  /// Log a key/value pair with the provided logger
  /**
     \param logger the StatLogger, previous passed to
     TupleSizeLoggingStrategyInterface::registerStats, to which to log
     information about this key/value pair

     \param kvPair the key/value pair to log
   */
  virtual void logTuple(StatLogger& logger, KeyValuePair& kvPair) = 0;
};

#endif // MAPRED_TUPLE_SIZE_LOGGING_STRATEGY_INTERFACE_H
