#ifndef THEMIS_INTERVAL_STAT_LOGGER_CLIENT_H
#define THEMIS_INTERVAL_STAT_LOGGER_CLIENT_H

#include "core/StatLogger.h"

class IntervalStatLoggerClient {
public:
  virtual ~IntervalStatLoggerClient() {}

  virtual StatLogger* initIntervalStatLogger() = 0;
  virtual void logIntervalStats(StatLogger& logger) const = 0;
};

#endif // THEMIS_INTERVAL_STAT_LOGGER_CLIENT_H
