#ifndef THEMIS_STAT_CONTAINER_H
#define THEMIS_STAT_CONTAINER_H

#include <stdint.h>
#include <string>

#include "core/StatContainerInterface.h"

/**
   A base class for stat containers that stores UIDs and stat names
 */
class StatContainer : public StatContainerInterface {
public:
  /// Constructor
  /**
     \param parentLoggerID the unique identifier of the StatLogger that is
     logging this statistic

     \param containerID the unique identifier of this stat container within its
     parent StatLogger

     \param statName the name of this statistic
  */
  StatContainer(
    uint64_t parentLoggerID, uint64_t containerID, const std::string& statName);

  // Destructor
  virtual ~StatContainer();

  /// \return the unique ID of this container's parent StatLogger
  uint64_t getParentLoggerID() const;

  /// \return the unique ID of this container within its parent StatLogger
  uint64_t getContainerID() const;

  /// \return the stat name for which this container is responsible
  const std::string& getStatName() const;
private:
  const uint64_t parentLoggerID;
  const uint64_t containerID;
  const std::string statName;
};


#endif // THEMIS_STAT_CONTAINER_H
