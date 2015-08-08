#ifndef TRITONSORT_DATUM_CONTAINER_H
#define TRITONSORT_DATUM_CONTAINER_H

#include <inttypes.h>
#include <list>
#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include "core/LogLineDescriptor.h"

class LoggableDatum;
class Timer;

/**
   Holds a collection of pieces of information called datums (that are not
   statistics, but should still be logged).
 */
class LogDataContainer {
public:
  /// Shorthand for a list of LogDataContainers
  typedef std::list<LogDataContainer> LogDataContainerList;

  /// Shorthand for an iterator through a LogDataContainerList
  typedef LogDataContainerList::iterator LogDataContainerListIter;

  /// Constructor
  /**
     \param loggerPrintPrefix the prefix that will be written to a log file
     before each data point in this container is written
   */
  LogDataContainer(const LogLineDescriptor& parentDescriptor);

  /// Destructor
  virtual ~LogDataContainer();

  /// Add a string datum to the container
  /**
     \param statName the name to associate with the datum
     \param datum the datum itself
   */
  void add(const std::string& statName, const std::string& datum);
  /// Add an unsigned integer datum to the container
  /**
     \param statName the name to associate with the datum
     \param datum the datum itself
   */
  void add(const std::string& statName, uint64_t datum);

  /// Add a timer datum to the container
  /**
     This method will log the start, stop and elapsed times of the timer

     \param statName the name to associate with the datum
     \param datum the datum itself
   */
  void add(const std::string& statName, const Timer& timer);

  /// Is the container empty?
  bool empty() const;

  /// Write the contents of this container to a file
  /**
     \param fileDescriptor an open file descriptor corresponding to the file to
     which to write

     \param phaseName the name of the phase during which this data was logged

     \param epoch the epoch of the phase during which this data was logged
   */
  void write(int fileDescriptor, const std::string& phaseName, uint64_t epoch);

  /// Insert description strings for all data logged to this container into the
  /// given set
  /**
     \param[out] descriptionSet a JSON array into which to insert this
     container's description objects
   */
  void addLogLineDescriptions(std::set<Json::Value>& descriptionSet);
private:
  typedef std::list<LoggableDatum*> LoggableDatumList;
  typedef std::map<std::string, LoggableDatumList > LoggableDatumMap;
  typedef LoggableDatumMap::iterator LoggableDatumMapIter;
  typedef std::map<std::string, LogLineDescriptor* > LogLineDescriptorMap;
  typedef LogLineDescriptorMap::iterator LogLineDescriptorMapIter;

  LogLineDescriptor baseLogLineDescriptor;

  LoggableDatumMap data;
  LogLineDescriptorMap descriptors;

  void add(const std::string& statName, LoggableDatum* datumObj);
};

#endif // TRITONSORT_DATUM_CONTAINER_H
