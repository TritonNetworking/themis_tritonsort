#ifndef TRITONSORT_STAT_WRITER_H
#define TRITONSORT_STAT_WRITER_H

#include <errno.h>
#include <json/json.h>
#include <list>
#include <map>
#include <pthread.h>
#include <set>
#include <stdint.h>
#include <string.h>
#include <string>

#include "core/LogDataContainer.h"
#include "core/Thread.h"
#include "core/ThreadSafeQueue.h"

class File;
class Params;

/**
   The statistic writer is responsible for periodically gathering statistic
   data from StatLoggers and writing that statistic data to disk. It is also
   responsible for garbage-collecting StatLoggers' statistic containers when
   the StatLoggers destruct.
 */
class StatWriter : private Thread {
public:
  /// Initialize the singleton StatWriter
  /**
     \sa StatWriter::StatWriter
   */
  static void init(Params& params);

  /// Stop the StatWriter thread
  static void teardown();

  /// Set the phase name associated with logged data
  /**
     \sa StatWriter::setPhaseName
   */
  static void setCurrentPhaseName(const std::string& phaseName);

  /// Set the epoch number associated with logged data
  /**
     \sa StatWriter::setEpoch
   */
  static void setCurrentEpoch(uint64_t epoch);

  /// Register a StatLogger with the singleton StatWriter
  /**
     \return the unique ID of the registrant that it will use to identify its
     containers, or 0 if no singleton StatWriter has been instantiated

     \sa StatWriter::registerStatLogger
   */
  static uint64_t registerLogger();

  /// Spawn the stat writing thread using the singleton StatWriter
  static void spawn();

  /// Add a stat container to the singleton StatWriter
  /**
     This method calls StatWriter::addStatContainerInternal on the singleton
     writer if it has been instantiated. If it has not been instantiated, the
     provided container is deleted.

     Thread-safe.

     \param statContainer the container to add
   */
  static void addStatContainer(StatContainerInterface* statContainer);

  /// Add a log data container to the singleton StatWriter
  /**
     This method calls StatWriter::addLogDataContainerInternal on the singleton
     writer if it has been instantiated. If it has not been instantiated, the
     provided container is deleted.

     \param logDataContainer the log data container to add
   */
  static void addLogDataContainer(LogDataContainer* logDataContainer);

  /// Add a log line descriptor to the singleton StatWriter
  /**
     This method calls StatWriter::addLogLineDescriptorInternal on the
     singleton writer if it has been instantiated. If it has not been
     instantiated, the provided container is deleted.

     \param loggerID the unique ID of the stat logger that is doing the logging

     \param statID the statistic ID of the statistic within the logger for
     which this log line descriptor applies

     \param logLineDescriptor the log line descriptor to add
   */
  static void addLogLineDescriptor(
    uint64_t loggerID, uint64_t statID, LogLineDescriptor* logLineDescriptor);

  /// Destructor
  virtual ~StatWriter();

  /// Register a StatLogger with this stat writer
  /**
     Called by StatLoggers at construction time.

     \return the unique ID of the registrant that it will use to identify its
     containers
   */
  uint64_t registerStatLogger();

  /// Set the phase name associated with logged data
  /**
     \param phaseName the phase name to associate with all statistics logged in
     the future
   */
  void setPhaseName(const std::string& phaseName);

  /// Set the epoch number associated with logged data
  /**
     \param epoch the number to associate with all statistics logged in
     the future
   */
  void setEpoch(uint64_t epoch);

  /// Stop the stat writing thread and join on the thread that was spawned by a
  /// previous call to StatWriter::spawnThread
  void teardownThread();

private:
  typedef std::list<LogDataContainer*> LogDataContainerList;

  typedef std::map< std::pair<uint64_t, uint64_t>, LogLineDescriptor* >
  LogLineDescriptorMap;

  /// Constructor
  /**
     \param params a Params object containing parameter information that
     StatWriter will use to initialize itself.
  */
  StatWriter(Params& params);

  void* thread(void* args);

  void safeMutexInit(pthread_mutex_t& mutex);
  void safeMutexDestroy(pthread_mutex_t& mutex);

  void drainLogDataContainerQueue();

  /// Add a stat container to this writer
  /**
     This writer is responsible for garbage-collecting the container.

     \param statContainer the container to add
   */
  void addStatContainerInternal(StatContainerInterface* statContainer);

  /// Add a log data container to this writer
  /**
     This writer is responsible for garbage-collecting the container.

     \param logDataContainer the container to add
   */
  void addLogDataContainerInternal(LogDataContainer* logDataContainer);

  /// Add a log line descriptor to this writer
  /**
     \param loggerID the unique ID of the stat logger that is doing the logging

     \param statID the statistic ID of the statistic within the logger for
     which this log line descriptor applies

     \param logLineDescriptor the log line descriptor to add
   */
  void addLogLineDescriptorInternal(
    uint64_t loggerID, uint64_t statID, LogLineDescriptor* logLineDescriptor);

  static pthread_mutex_t singletonWriterLock;
  static StatWriter* singletonWriter;

  // StatLoggers are assigned unique IDs starting from 1. The ID 0 is assigned
  // to all loggers that attempt to register with a nonexistent singleton
  // StatWriter
  pthread_mutex_t nextStatLoggerIDLock;
  uint64_t nextStatLoggerID;

  ThreadSafeQueue<StatContainerInterface*> statContainerQueue;
  ThreadSafeQueue<LogDataContainer*> logDataContainerQueue;

  pthread_mutex_t logLineDescriptorMapLock;
  LogLineDescriptorMap logLineDescriptorMap;

  const Params& params;

  pthread_mutex_t phaseAndEpochLock;

  pthread_cond_t phaseAndEpochChanged;

  std::string currentPhaseName;
  std::string* nextPhaseName;

  uint64_t currentEpoch;
  uint64_t* nextEpoch;

  pthread_mutex_t runningWriterStateLock;
  bool stopWriter;
  bool writerRunning;

  File* logFile;
  File* descriptorsFile;

  std::set<Json::Value> logLineDescriptionSet;
};

#endif // TRITONSORT_STAT_WRITER_H
