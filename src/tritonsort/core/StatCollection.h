#ifndef TRITONSORT_STAT_COLLECTION_H
#define TRITONSORT_STAT_COLLECTION_H

#include <limits>
#include <list>
#include <pthread.h>
#include <stdint.h>

#include "core/LogLineDescriptor.h"
#include "core/StatContainer.h"
#include "core/StatSummary.h"

/**
   Collects an uncompressed time-series stream of unsigned integers
 */
class StatCollection : public StatContainer {
public:
  /// Constructor
  /**
     \param parentLoggerID the unique identifier of the StatLogger that is
     logging this statistic

     \param containerID the unique identifier of this stat container within its
     parent StatLogger

     \param statName the name of this statistic

     \param threshold stats less than this threshold will not be logged
     (however, they will still contribute to summary statistics)
   */
  StatCollection(
    uint64_t parentLoggerID, uint64_t containerID, const std::string& statName,
    uint64_t threshold = 0);

  /// Destructor
  virtual ~StatCollection();

  void setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor);

  StatCollection* newEmptyCopy() const;

  void add(uint64_t stat);

  void add(const Timer& timer) {
    add(timer.getElapsed());
  }

  void writeStatsToFile(
    File& file, LogLineDescriptor& logLineDescriptor,
    const std::string& phaseName, uint64_t epoch) const;

  void addLogLineDescriptions(std::set<Json::Value>& descriptionSet) const;

  bool isReadyForWriting() const;

private:
  const uint64_t threshold;

  std::list<uint64_t> statQueue;
  std::list<uint64_t> timestampQueue;

  StatCollection(const StatCollection& other);
};

#endif // TRITONSORT_STAT_COLLECTION_H
