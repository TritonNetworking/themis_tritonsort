#ifndef TRITONSORT_STAT_CONTAINER_INTERFACE_H
#define TRITONSORT_STAT_CONTAINER_INTERFACE_H

#include <json/json.h>"
#include <list>
#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include "core/File.h"
#include "core/LogLineDescriptor.h"
#include "core/Timer.h"

/**
   The interface that all statistic containers used by StatWriter must provide
 */
class StatContainerInterface {
public:
  /// Destructor
  virtual ~StatContainerInterface() {}

  /// \return the unique identifier of this container's parent StatLogger
  virtual uint64_t getParentLoggerID() const = 0;

  /// \return the unique identifier of this container within its parent StatLogger
  virtual uint64_t getContainerID() const = 0;

  /// \return a new, empty copy of this container
  virtual StatContainerInterface* newEmptyCopy() const = 0;

  /// Write this container's statistics to a file
  /**
     \param file the file to which to write

     \param logLineDescriptor a LogLineDescriptor that will be used to format
     stats for writing

     \param phaseName the name of the phase for which stats are currently being
     written

     \param epoch the epoch for which stats are currently being written
   */
  virtual void writeStatsToFile(
    File& file, LogLineDescriptor& logLineDescriptor,
    const std::string& phaseName, uint64_t epoch) const = 0;


  /// Add a statistic to the collection and record the time when that statistic
  /// was added.
  /**
     \param stat the statistic to add
  */
  virtual void add(uint64_t stat) = 0;


  /// Add the elapsed time of the given timer to the collection and record the
  /// time when that statistic was added.
  /**
     \param timer the timer whose elapsed time to add
   */
  virtual void add(const Timer& timer) = 0;

  /// Add fields to a parent LogLineDescriptor so that it can properly write
  /// stats for this container
  /**
     \param logLineDescriptor the descriptor to modify
   */
  virtual void setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor) = 0;

  /// \return true if this stat container is ready to be written to disk by the
  /// StatWriter, false otherwise
  virtual bool isReadyForWriting() const = 0;
};

#endif // TRITONSORT_STAT_CONTAINER_INTERFACE_H
