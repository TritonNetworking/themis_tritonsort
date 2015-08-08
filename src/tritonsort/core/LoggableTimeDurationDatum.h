#ifndef TRITONSORT_LOGGABLE_TIME_DURATION_DATUM_H
#define TRITONSORT_LOGGABLE_TIME_DURATION_DATUM_H

#include <stdio.h>

#include "core/LoggableDatum.h"
#include "core/Timer.h"

/**
   A loggable datum for time durations
 */
class LoggableTimeDurationDatum : public LoggableDatum {
public:
  /// Constructor
  /**
     \param timer a timer whose start, stop and elapsed times will be logged
   */
  LoggableTimeDurationDatum(const Timer& timer);

  /// Write this datum to a file
  /// \sa LoggableDatum::write
  void write(int fileDescriptor, const std::string& phaseName, uint64_t epoch,
             const LogLineDescriptor& descriptor);

  /// \sa LoggableDatum::createLogLineDescriptor
  LogLineDescriptor* createLogLineDescriptor(
    const LogLineDescriptor& parentDescriptor, const std::string& statName)
    const;
private:
  uint64_t startTime;
  uint64_t stopTime;
  uint64_t elapsedTime;
};


#endif // TRITONSORT_LOGGABLE_TIME_DURATION_DATUM_H
