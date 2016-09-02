#define __STDC_FORMAT_MACROS 1

#include <algorithm>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>

#include "ScopedLock.h"
#include "StatCollection.h"
#include "StatSummary.h"
#include "StatusPrinter.h"
#include "Timer.h"
#include "TritonSortAssert.h"
#include "Utils.h"

StatCollection::StatCollection(
  uint64_t parentLoggerID, uint64_t containerID, const std::string& statName,
  uint64_t _threshold)
  : StatContainer(parentLoggerID, containerID, statName),
    threshold(_threshold) {
}

StatCollection::StatCollection(const StatCollection& other)
  : StatContainer(
    other.getParentLoggerID(), other.getContainerID(),
    other.getStatName()),
    threshold(other.threshold) {
}

StatCollection::~StatCollection() {
}

void StatCollection::setupLogLineDescriptor(
  LogLineDescriptor& logLineDescriptor) {

  logLineDescriptor.setLogLineTypeName("COLL")
    .addConstantStringField("collection_stat_name", getStatName())
    .addField("timestamp", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("value", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();
}

StatCollection* StatCollection::newEmptyCopy() const {
  return new StatCollection(*this);
}

void StatCollection::add(uint64_t stat) {
  if (threshold == 0 || stat >= threshold) {
    bool bad_alloc = false;
    try {
      statQueue.push_back(stat);
      timestampQueue.push_back(Timer::posixTimeInMicros());
    } catch (std::bad_alloc&) {
      bad_alloc = true;
    }

    ABORT_IF(bad_alloc, "Ran out of memory while pushing a statistic");
  }
}

bool StatCollection::isReadyForWriting() const {
  // Should only bother writing this container if there are stats in it
  return statQueue.size() > 0;
}

void StatCollection::writeStatsToFile(
  File& file, LogLineDescriptor& logLineDescriptor,
  const std::string& phaseName, uint64_t epoch) const {

  TRITONSORT_ASSERT(statQueue.size() == timestampQueue.size(), "There needs to be a "
         "timestamp for every statistic logged by a StatCollection");

  int fileDescriptor = file.getFileDescriptor();

  std::list<uint64_t>::const_iterator statIter = statQueue.begin();
  std::list<uint64_t>::const_iterator timestampIter = timestampQueue.begin();

  const char* phaseNameCStr = phaseName.c_str();

  while (statIter != statQueue.end()) {
    logLineDescriptor.writeLogLineToFile(
      fileDescriptor, phaseNameCStr, epoch, *timestampIter, *statIter);

    statIter++;
    timestampIter++;
  }
}
