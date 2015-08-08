#ifndef TRITONSORT_THRESHOLDED_STAT_COLLECTION_H
#define TRITONSORT_THRESHOLDED_STAT_COLLECTION_H

#include <pthread.h>

#include "core/LogLineDescriptor.h"
#include "core/StatContainer.h"
#include "core/StatSummary.h"

/**
   A TimerStatCollection collects statistics about timers. In particular, it
   captures start timestamp, end timestamp and elapsed time for all timers
   whose elapsed time is above the provided threshold, as well as elapsed time
   summary statistics.
 */
class TimerStatCollection : public StatContainer {
public:
  TimerStatCollection(
    uint64_t parentLoggerID, uint64_t collectionID, const std::string& statName,
    uint64_t threshold = 0);

  /// Destructor
  virtual ~TimerStatCollection();

  void setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor);

  TimerStatCollection* newEmptyCopy() const;

  /// \cond PRIVATE
  void add(uint64_t stat);
  /// \endcond

  void add(const Timer& timer);

  void writeStatsToFile(
    File& file, LogLineDescriptor& logLineDescriptor,
    const std::string& phaseName, uint64_t epoch) const;

  void addLogLineDescriptions(std::set<Json::Value>& descriptionSet) const;

  bool isReadyForWriting() const;
private:
  const uint64_t threshold;
  std::string baseStatName;

  std::list<uint64_t> startTimes;
  std::list<uint64_t> stopTimes;
  std::list<uint64_t> elapsedTimes;

  TimerStatCollection(const TimerStatCollection& other);
};

#endif // TRITONSORT_THRESHOLDED_STAT_COLLECTION_H
