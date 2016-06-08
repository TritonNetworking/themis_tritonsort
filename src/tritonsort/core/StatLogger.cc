#include <limits>
#include <sstream>

#include "StatLogger.h"
#include "StatWriter.h"
#include "StatusPrinter.h"
#include "tritonsort/config.h"
#include "core/LogDataContainer.h"
#include "core/MemoryUtils.h"
#include "core/Params.h"
#include "core/ScopedLock.h"
#include "core/StatCollection.h"
#include "core/StatHistogram.h"
#include "core/StatSummary.h"
#include "core/TimerStatCollection.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

StatLogger::StatLogger(const std::string& loggerName) {
  logLineDescriptor.addConstantStringField("logger_name", loggerName);
  init();
}

StatLogger::StatLogger(const std::string& stageOrPoolName, uint64_t id) {
  logLineDescriptor
    .addConstantStringField("stage_name", stageOrPoolName)
    .addConstantUIntField("id", id);
  init();
}

StatLogger::StatLogger(const std::string& stageName, uint64_t id,
                       const std::string& poolName, uint64_t poolNumber) {
  logLineDescriptor
    .addConstantStringField("stage_name", stageName)
    .addConstantUIntField("worker_id", id)
    .addConstantStringField("pool_name", poolName)
    .addConstantUIntField("pool_number", poolNumber);

  init();
}

StatLogger::~StatLogger() {
  // Drain all remaining statistics
  for (StatContainerMapIter iter = stats.begin(); iter != stats.end(); iter++) {
    StatWriter::addStatContainer(iter->second);
  }
  stats.clear();

  // Drain summary statistics
  for (StatContainerMapIter iter = summaryStats.begin();
       iter != summaryStats.end(); iter++) {
    StatWriter::addStatContainer(iter->second);
  }
  summaryStats.clear();

  // Give the stat writer your log data container
  if (data != NULL) {
    StatWriter::addLogDataContainer(data);
    data = NULL;
  }
}

void StatLogger::init() {
  largestRegisteredStatID = 0;
  data = new (themis::memcheck) LogDataContainer(logLineDescriptor);

  statLoggerID = StatWriter::registerLogger();

  setNextStatPushTime();
}

uint64_t StatLogger::registerStat(const std::string& statName,
                                  uint64_t threshold) {
#ifdef TRITONSORT_STATS
  uint64_t statID = largestRegisteredStatID++;

  StatCollection* collection = new (themis::memcheck) StatCollection(
    statLoggerID, statID, statName, threshold);

  addStatContainer(collection, statName, true);
  return statID;
#else
  // Adding a dummy return statement here to appease the compiler
  return std::numeric_limits<uint64_t>::max();
#endif //TRITONSORT_STATS
}

uint64_t StatLogger::registerTimerStat(const std::string& statName,
                                       uint64_t threshold) {
#ifdef TRITONSORT_STATS
  uint64_t statID = largestRegisteredStatID++;

  TimerStatCollection* collection = new (themis::memcheck) TimerStatCollection(
    statLoggerID, statID, statName, threshold);

  addStatContainer(collection, statName, true);
  return statID;
#else
  // Adding a dummy return statement here to appease the compiler
  return std::numeric_limits<uint64_t>::max();
#endif //TRITONSORT_STATS
}

uint64_t StatLogger::registerHistogramStat(const std::string& statName,
                                           uint64_t binSize) {
#ifdef TRITONSORT_STATS
  uint64_t statID = largestRegisteredStatID++;

  StatHistogram* histogram = new (themis::memcheck) StatHistogram(
    statLoggerID, statID, statName, binSize);

  addStatContainer(histogram, statName, true);
  return statID;
#else
  // Adding a dummy return statement here to appease the compiler
  return std::numeric_limits<uint64_t>::max();
#endif //TRITONSORT_STATS
}

uint64_t StatLogger::registerSummaryStat(const std::string& statName) {
#ifdef TRITONSORT_STATS
  uint64_t statID = largestRegisteredStatID++;

  StatSummary* summary = new (themis::memcheck) StatSummary(
    statLoggerID, statID, statName);

  addStatContainer(summary, statName, false);
  return statID;
#else
  // Adding a dummy return statement here to appease the compiler
  return std::numeric_limits<uint64_t>::max();
#endif // TRITONSORT_STATS
}

void StatLogger::add(uint64_t statID, uint64_t stat) {
#ifdef TRITONSORT_STATS
  addStatToContainer<uint64_t>(statID, stat);
#endif //TRITONSORT_STATS
}

void StatLogger::add(uint64_t statID, const Timer& timer) {
#ifdef TRITONSORT_STATS
  addStatToContainer<Timer>(statID, timer);
#endif //TRITONSORT_STATS
}

void StatLogger::logDatum(const std::string& statName,
                          const std::string& datum) {
#ifdef TRITONSORT_STATS
  data->add(statName, datum);
#endif //TRITONSORT_STATS
}

void StatLogger::logDatum(const std::string& statName, uint64_t datum) {
#ifdef TRITONSORT_STATS
  data->add(statName, datum);
#endif //TRITONSORT_STATS
}

void StatLogger::logDatum(const std::string& statName, const Timer& datum) {
#ifdef TRITONSORT_STATS
  data->add(statName, datum);
#endif //TRITONSORT_STATS
}

void StatLogger::addStatContainer(
  StatContainerInterface* statContainer, const std::string& statName,
  bool createSummary) {

  // Allow for fast lookup based on stat container's container ID
  uint64_t statID = statContainer->getContainerID();
  stats[statID] = statContainer;

  // Give the log line descriptor for this stat to the writer, constructing it
  // based on the logger's descriptor
  LogLineDescriptor* statLogLineDescriptor =
    new (themis::memcheck) LogLineDescriptor(logLineDescriptor);
  statContainer->setupLogLineDescriptor(*statLogLineDescriptor);

  StatWriter::addLogLineDescriptor(statLoggerID, statID, statLogLineDescriptor);

  if (createSummary) {
    // Create a summary stat collection for this statistic
    uint64_t summaryStatID = largestRegisteredStatID++;
    StatSummary* summaryStat = new (themis::memcheck) StatSummary(
      statLoggerID, summaryStatID, statName);

    // Associate the summary with the original container's stat ID so that we
    // can easily retrieve both at once
    summaryStats[statID] = summaryStat;

    // Give the summary's log line descriptor to the writer as well
    LogLineDescriptor* summaryLogLineDescriptor =
      new (themis::memcheck) LogLineDescriptor(logLineDescriptor);

    summaryStat->setupLogLineDescriptor(*summaryLogLineDescriptor);
    StatWriter::addLogLineDescriptor(
      statLoggerID, summaryStatID, summaryLogLineDescriptor);
  }
}
