#ifndef TRITONSORT_STAT_LOGGER_H
#define TRITONSORT_STAT_LOGGER_H

#include <map>
#include <queue>
#include <sstream>
#include <stdint.h>
#include <string>
#include <vector>

#include "core/LogLineDescriptor.h"
#include "core/StatContainerInterface.h"
#include "StatWriter.h"
#include "core/TritonSortAssert.h"

class LogDataContainer;
class Params;

/**
   Provides a convenient wrapper for logging unsigned integer statistics using
   stat containers that implement StatContainerInterface.
 */
class StatLogger {
private:
  typedef std::map<uint64_t, StatContainerInterface*> StatContainerMap;
  typedef StatContainerMap::iterator StatContainerMapIter;
  typedef StatContainerMap::const_iterator StatContainerMapConstIter;

public:
  /// Shorthand for a list of stat containers
  typedef std::list<StatContainerInterface*> StatContainerList;

  /// Shorthand for an iterator through a list of stat containers
  typedef StatContainerList::iterator StatContainerListIter;

  /// Shorthand for a const iterator through a list of stat containers
  typedef StatContainerList::const_iterator StatContainerListConstIter;

  /// Constructor
  /**
     \param loggerName a string name describing this logger
   */
  StatLogger(const std::string& loggerName);

  /// Constructor
  /**
     \param stageOrPoolName the name of the stage or resource pool using this
     StatLogger

     \param id the worker or pool id of the stage or resource pool using the
     logger
   */
  StatLogger(const std::string& stageOrPoolName, uint64_t id);

  /// Constructor
  /**
     StatLoggers constructed using this interface are used to log the
     interaction between a particular worker for a stage and a particular
     resource pool.

     \param stageName the name of the stage

     \param id the worker ID of the worker

     \param poolName the name of the pool

     \param poolNumber the pool ID of the pool
   */
  StatLogger(const std::string& stageName, uint64_t id,
             const std::string& poolName, uint64_t poolNumber);

  /// Destructor
  virtual ~StatLogger();

  /// Register a statistic with this stat logger
  /**
     Subsequent calls to StatLogger::add for the desired statistic must provide
     that statistic's ID.

     Caller is required to specify an ID distinct from all
     previously-registered IDs; not doing so will result in an exception.

     \param statName the name of this statistic

     \param threshold the smallest value for the statistic that will be
     recorded; statistics logged by StatLogger::add that fall below the
     threshold will not be logged individually, but will contribute to summary
     statistics

     \return the unique ID of this statistic, used in subsequent calls to
     StatLogger::add
   */
  uint64_t registerStat(const std::string& statName, uint64_t threshold = 0);

  /// Register a statistic with this stat logger
  /**
     Subsequent calls to StatLogger::add for the desired statistic must provide
     that statistic's ID.

     Caller is required to specify an ID distinct from all
     previously-registered IDs; not doing so will result in an exception.

     \param statName the name of this statistic

     \param threshold the smallest value for the timer's elapsed time that will
     be recorded; statistics logged by StatLogger::add that fall below the
     threshold will not be logged individually, but will contribute to summary
     statistics

     \return the unique ID of this statistic, used in subsequent calls to
     StatLogger::add
   */
  uint64_t registerTimerStat(const std::string& statName,
                             uint64_t threshold = 0);

  /// Register a statistic with this stat logger that is backed by a histogram
  /**
     Subsequent calls to StatLogger::add for the desired statistic must provide
     that statistic's ID.

     Caller is required to specify an ID distinct from all
     previously-registered IDs; not doing so will result in an exception.

     \param statName the name of this statistic

     \param binSize the size of the bins used by the histogram

     \return the unique ID of this statistic, used in subsequent calls to
     StatLogger::add
   */
  uint64_t registerHistogramStat(const std::string& statName, uint64_t binSize);

  /// Register a statistic with this stat logger for which you will only log
  /// summary statistics
  /**
     Subsequent calls to StatLogger::add for the desired statistic must provide
     that statistic's ID.

     Caller is required to specify an ID distinct from all
     previously-registered IDs; not doing so will result in an exception.

     \param statName the name of this statistic

     \return the unique ID of this statistic, used in subsequent calls to
     StatLogger::add
   */
  uint64_t registerSummaryStat(const std::string& statName);

  /// Add a value to the given statistic
  /**
     Any values that fall below the threshold provided at registration (see
     StatLogger::registerStat) will not be recorded.

     \param statID the unique ID of this statistic
     \param stat the statistic to log
   */
  void add(uint64_t statID, uint64_t stat);

  /// Add a value to the given statistic
  /**
     Any values that fall below the threshold provided at registration (see
     StatLogger::registerStat) will not be recorded.

     \param statID the unique ID of this statistic
     \param timer the timer whose start, end and elapsed times will be logged
   */
  void add(uint64_t statID, const Timer& timer);

  /// Log a datum
  /**
     A datum is a piece of information that is assumed to be logged
     infrequently (usually only once) and is meant to be stored in its raw form
     without any thresholding, aggregation, etc.

     \param statName a string name describing the datum

     \param datum the value of the datum to log
   */
  void logDatum(const std::string& statName, const std::string& datum);

  /// Log a datum
  /**
     A datum is a piece of information that is assumed to be logged
     infrequently (usually only once) and is meant to be stored in its raw form
     without any thresholding, aggregation, etc.

     \param statName a string name describing the datum

     \param datum the value of the datum to log
   */
  void logDatum(const std::string& statName, uint64_t datum);

  /// Log a datum
  /**
     A datum is a piece of information that is assumed to be logged
     infrequently (usually only once) and is meant to be stored in its raw form
     without any thresholding, aggregation, etc.

     \param statName a string name describing the datum

     \param datum the value of the datum to log
   */
  void logDatum(const std::string& statName, const Timer& datum);

private:
  uint64_t largestRegisteredStatID;

  LogLineDescriptor logLineDescriptor;

  uint64_t statLoggerID;

  StatContainerMap stats;
  StatContainerMap summaryStats;

  uint64_t nextStatPushTime;

  LogDataContainer* data;

  void init();

  /// \todo(AR) These constants should be configurable.
  inline void setNextStatPushTime() {
    nextStatPushTime = Timer::posixTimeInMicros() + 500000 +
      (lrand48() % 1000000);
  }

  template <typename T> void addStatToContainer(
    uint64_t statID, const T& stat) {

    StatContainerMapIter iter = stats.find(statID);

    ABORT_IF(iter == stats.end(), "No statistic with id %llu registered in "
             "this logger", statID);

    StatContainerInterface* statContainer = iter->second;
    statContainer->add(stat);

    iter = summaryStats.find(statID);

    if (iter != summaryStats.end()) {
      (iter->second)->add(stat);
    }

    // Push all stats if we're past the next stat push time
    if (Timer::posixTimeInMicros() > nextStatPushTime) {
      for (iter = stats.begin(); iter != stats.end(); iter++) {
        StatContainerInterface*& statContainer = iter->second;

        if (statContainer->isReadyForWriting()) {
          StatContainerInterface* emptyStatContainerCopy =
            statContainer->newEmptyCopy();

          StatWriter::addStatContainer(statContainer);
          statContainer = emptyStatContainerCopy;
        }
      }

      setNextStatPushTime();
    }
  }

  void addStatContainer(
    StatContainerInterface* container, const std::string& statName,
    bool createSummary);
};

#endif // TRITONSORT_STAT_LOGGER_H
