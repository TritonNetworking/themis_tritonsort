#ifndef TRITONSORT_LOGGABLE_UINT64_DATUM_H
#define TRITONSORT_LOGGABLE_UINT64_DATUM_H

#include <stdint.h>

#include "core/LoggableDatum.h"

/**
   A loggable datum for unsigned 64-bit integers
 */
class LoggableUInt64Datum : public LoggableDatum {
public:
  /// Constructor
  /**
     \param datum an unsigned 64-bit integer that will be logged
   */
  LoggableUInt64Datum(uint64_t datum);

  /// Write this datum to a file
  /// \sa LoggableDatum::write
  void write(int fileDescriptor, const std::string& phaseName, uint64_t epoch,
             const LogLineDescriptor& descriptor);

  /// \sa LoggableDatum::createLogLineDescriptor
  LogLineDescriptor* createLogLineDescriptor(
    const LogLineDescriptor& parentDescriptor, const std::string& statName)
    const;
private:
  const uint64_t datum;
};

#endif // TRITONSORT_LOGGABLE_UINT64_DATUM_H
