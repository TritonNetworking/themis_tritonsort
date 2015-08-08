#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>
#include "TimerStatCollection.h"
#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"

TimerStatCollection::TimerStatCollection(
  uint64_t parentLoggerID, uint64_t collectionID, const std::string& statName,
  uint64_t _threshold)
  : StatContainer(parentLoggerID, collectionID, statName),
    threshold(_threshold),
    baseStatName(statName) {
}

TimerStatCollection::TimerStatCollection(const TimerStatCollection& other)
  : StatContainer(other.getParentLoggerID(), other.getContainerID(),
                  other.getStatName()),
    threshold(other.threshold),
    baseStatName(other.baseStatName) {
}

TimerStatCollection::~TimerStatCollection() {
}

void TimerStatCollection::setupLogLineDescriptor(
  LogLineDescriptor& logLineDescriptor) {

  logLineDescriptor.setLogLineTypeName("TIMR")
    .addConstantStringField("stat_name", baseStatName)
    .addField("start_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("stop_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("elapsed_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();
}


TimerStatCollection* TimerStatCollection::newEmptyCopy() const {
  return new TimerStatCollection(*this);
}

void TimerStatCollection::add(uint64_t stat) {
  ABORT("Should only add Timers to TimerStatCollection");
}

void TimerStatCollection::add(const Timer& timer) {
  uint64_t elapsedTime = timer.getElapsed();

  if (threshold == 0 || elapsedTime >= threshold) {
    startTimes.push_back(timer.getStartTime());
    stopTimes.push_back(timer.getStopTime());
    elapsedTimes.push_back(elapsedTime);
  }
}

bool TimerStatCollection::isReadyForWriting() const {
  // Should only bother writing this collection if we have stats to write
  return startTimes.size() > 0;
}

void TimerStatCollection::writeStatsToFile(
  File& file, LogLineDescriptor& logLineDescriptor,
  const std::string& phaseName, uint64_t epoch) const {

  ASSERT(startTimes.size() == stopTimes.size() &&
         stopTimes.size() == elapsedTimes.size(),
         "Stop, start and elapsed time lists should be the same length");

  int fileDescriptor = file.getFileDescriptor();

  std::list<uint64_t>::const_iterator startIter = startTimes.begin();
  std::list<uint64_t>::const_iterator stopIter = stopTimes.begin();
  std::list<uint64_t>::const_iterator elapsedIter = elapsedTimes.begin();

  const char* phaseNameCStr = phaseName.c_str();

  while (startIter != startTimes.end()) {
    logLineDescriptor.writeLogLineToFile(
      fileDescriptor, phaseNameCStr, epoch, *startIter, *stopIter,
      *elapsedIter);

    startIter++;
    stopIter++;
    elapsedIter++;
  }
}
