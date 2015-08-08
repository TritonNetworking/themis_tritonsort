#ifndef TRITONSORT_STAT_HISTOGRAM_H
#define TRITONSORT_STAT_HISTOGRAM_H

#include <map>
#include <pthread.h>

#include "core/LogLineDescriptor.h"
#include "core/StatContainer.h"
#include "core/StatSummary.h"

/**
   StatHistogram stores a histogram of values logged for a given statistic.
 */
class StatHistogram : public StatContainer {
public:
  StatHistogram(
    uint64_t parentLoggerID, uint64_t containerID,
    const std::string& statName, uint64_t binSize);

  /// Destructor
  virtual ~StatHistogram();

  void setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor);

  StatHistogram* newEmptyCopy() const;

  void add(uint64_t stat);

  void add(const Timer& timer) {
    add(timer.getElapsed());
  }

  void writeStatsToFile(
    File& file, LogLineDescriptor& logLineDescriptor,
    const std::string& phaseName, uint64_t epoch) const;

  bool isReadyForWriting() const;

private:
  typedef std::map<uint64_t, uint64_t> BinMap;
  typedef BinMap::iterator BinMapIter;

  const uint64_t binSize;

  BinMap bins;

  StatHistogram(const StatHistogram& other);
};

#endif // TRITONSORT_STAT_HISTOGRAM_H
