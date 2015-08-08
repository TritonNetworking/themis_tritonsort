#ifndef TRITONSORT_LOGGABLE_STRING_DATUM_H
#define TRITONSORT_LOGGABLE_STRING_DATUM_H

#include <stdio.h>

#include "core/LoggableDatum.h"
#include "core/TritonSortAssert.h"

/**
   A loggable datum for strings
 */
class LoggableStringDatum : public LoggableDatum {
public:
  /// Constructor
  /**
     \param a string that will be logged
   */
  LoggableStringDatum(const std::string& datum);

  /// Write this datum to a file
  /// \sa LoggableDatum::write
  void write(int fileDescriptor, const std::string& phaseName, uint64_t epoch,
             const LogLineDescriptor& descriptor);

  /// \sa LoggableDatum::createLogLineDescriptor
  LogLineDescriptor* createLogLineDescriptor(
    const LogLineDescriptor& parentDescriptor, const std::string& statName)
    const;
private:
  std::string datum;
};


#endif // TRITONSORT_LOGGABLE_STRING_DATUM_H
