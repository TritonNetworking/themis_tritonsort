#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>
#include <limits>
#include <math.h>

#include "core/LogLineFieldInfo.h"
#include "core/ScopedLock.h"
#include "core/StatSummary.h"

StatSummary::StatSummary(
  uint64_t parentLoggerID, uint64_t containerID, const std::string& statName)
  : StatContainer(parentLoggerID, containerID, statName) {
  init();
}

StatSummary::StatSummary(const StatSummary& other)
  : StatContainer(
    other.getParentLoggerID(), other.getContainerID(), other.getStatName()),
    count(other.count),
    sum(other.sum),
    streaming_mean(other.streaming_mean),
    sum_of_squares_of_mean_differences(
      other.sum_of_squares_of_mean_differences),
    min(other.min),
    max(other.max) {
}

StatSummary::~StatSummary() {
}

void StatSummary::setupLogLineDescriptor(LogLineDescriptor& logLineDescriptor) {
  logLineDescriptor.setLogLineTypeName("SUMM")
    .addConstantStringField("stat_name", getStatName())
    .addField("summary_stat_name", LogLineFieldInfo::STRING)
    .addField("value", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();
}

void StatSummary::init() {
  sum = 0;
  count = 0;
  streaming_mean = 0;
  sum_of_squares_of_mean_differences = 0;
  min = std::numeric_limits<uint64_t>::max();
  max = std::numeric_limits<uint64_t>::min();
}

StatSummary* StatSummary::newEmptyCopy() const {
  return new StatSummary(*this);
}

void StatSummary::writeStatsToFile(
  File& file, LogLineDescriptor& logLineDescriptor,
  const std::string& phaseName, uint64_t epoch) const {

  const char* phaseNameCStr = phaseName.c_str();

  int fileDescriptor = file.getFileDescriptor();

  uint64_t mean = floor(streaming_mean);
  uint64_t variance = sum_of_squares_of_mean_differences / count;

  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "min", min);
  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "max", max);
  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "sum", sum);
  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "count", count);
  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "mean", mean);
  logLineDescriptor.writeLogLineToFile(
    fileDescriptor, phaseNameCStr, epoch, "variance", variance);
}

bool StatSummary::isReadyForWriting() const {
  // Since this is a summary statistic, it should only be written at the end of
  // the run when writing it becomes necessary to avoid losing it
  return false;
}
