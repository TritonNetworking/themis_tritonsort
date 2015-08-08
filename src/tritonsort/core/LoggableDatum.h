#ifndef TRITONSORT_LOGGABLE_DATUM_H
#define TRITONSORT_LOGGABLE_DATUM_H

#include <string>

class LogLineDescriptor;

/**
   The base class from which all datum that are stored in LoggableDataContainer
   must derive.
 */
class LoggableDatum {
public:
  /// Destructor
  virtual ~LoggableDatum() {}

  /// Write this datum to a file
  /**
     \param fileDescriptor an open file descriptor to which to write

     \param phaseName the name associated with the datum

     \param epoch the epoch number associated with the datum

     \param descriptor the LogLineDescriptor corresponding to this datum
   */
  virtual void write(
    int fileDescriptor, const std::string& phaseName, uint64_t epoch,
    const LogLineDescriptor& descriptor) = 0;

  /// Generate a log line descriptor based on your parent's log line descriptor
  /**
     \param parentDescriptor a pointer to your parent's LogLineDescriptor
     \param statName the name of the datum that this LoggableDatum is logging
   */
  virtual LogLineDescriptor* createLogLineDescriptor(
    const LogLineDescriptor& parentDescriptor, const std::string& statName)
    const = 0;
};

#endif // TRITONSORT_LOGGABLE_DATUM_H
