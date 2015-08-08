#ifndef TRITONSORT_STAT_SUMMARY_H
#define TRITONSORT_STAT_SUMMARY_H

#include <stdint.h>
#include <string>
#include <pthread.h>

#include "core/LogLineDescriptor.h"
#include "core/StatContainer.h"
#include "core/TritonSortAssert.h"

/**
   A StatSummary logs summary statistics (min, max, sum, count, population
   variance) for a set of statistic values.

   Uses a streaming method of calculating mean and population variance
   described here:

   http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
   #On-line_algorithm
 */
class StatSummary : public StatContainer {
public:
  StatSummary(
    uint64_t parentLoggerID, uint64_t containerID, const std::string& statName);

  /// Destructor
  virtual ~StatSummary();

  void setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor);

  /**
     This method is kind of a misnomer, since StatSummaries are considered to
     be cumulative and will only be written once they have been finalized.
   */
  StatSummary* newEmptyCopy() const;

  bool isReadyForWriting() const;

  inline void add(uint64_t stat) {
    sum += stat;
    count++;

    double delta = stat - streaming_mean;

    streaming_mean += delta / count;

    sum_of_squares_of_mean_differences += delta * (stat - streaming_mean);

    min = std::min<uint64_t>(stat, min);
    max = std::max<uint64_t>(stat, max);
  }

  inline void add(const Timer& timer) {
    add(timer.getElapsed());
  }

  void writeStatsToFile(
    File& file, LogLineDescriptor& logLineDescriptor,
    const std::string& phaseName, uint64_t epoch) const;

private:
  StatSummary(const StatSummary& other);
  void init();

  uint64_t count;
  uint64_t sum;
  double streaming_mean;
  double sum_of_squares_of_mean_differences;
  uint64_t min;
  uint64_t max;
};

#endif // TRITONSORT_STAT_SUMMARY_H
