#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>
#include <sstream>

#include "ScopedLock.h"
#include "StatHistogram.h"
#include "StatusPrinter.h"
#include "Timer.h"
#include "TritonSortAssert.h"

StatHistogram::StatHistogram(
  uint64_t parentLoggerID, uint64_t containerID, const std::string& statName,
  uint64_t _binSize)
  : StatContainer(parentLoggerID, containerID, statName),
    binSize(_binSize) {

  ABORT_IF(binSize == 0, "Bin size cannot be zero");
}

StatHistogram::~StatHistogram() {
}

void StatHistogram::setupLogLineDescriptor(
  LogLineDescriptor& logLineDescriptor) {

  logLineDescriptor.setLogLineTypeName("HIST")
    .addConstantStringField("stat_name", getStatName())
    .addField("bin", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("count", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();
}

StatHistogram::StatHistogram(const StatHistogram& other)
  : StatContainer(
    other.getParentLoggerID(), other.getContainerID(),
    other.getStatName()),
    binSize(other.binSize) {
}

StatHistogram* StatHistogram::newEmptyCopy() const {
  return new StatHistogram(*this);
}

void StatHistogram::add(uint64_t stat) {
  bool bad_alloc = false;

  uint64_t bin = stat / binSize;

  BinMapIter iter = bins.find(bin);

  if (iter != bins.end()) {
    iter->second++;
  } else {
    try {
      bins[bin] = 1;
    } catch (std::bad_alloc&) {
      bad_alloc = true;
    }
  }

  ABORT_IF(bad_alloc, "Ran out of memory while pushing a statistic");
}

void StatHistogram::writeStatsToFile(
  File& file, LogLineDescriptor& logLineDescriptor,
  const std::string& phaseName, uint64_t epoch) const {

  const char* phaseNameCStr = phaseName.c_str();

  int fileDescriptor = file.getFileDescriptor();

  for (BinMap::const_iterator iter = bins.begin(); iter != bins.end(); iter++) {
    logLineDescriptor.writeLogLineToFile(
      fileDescriptor, phaseNameCStr, epoch, iter->first * binSize,
      iter->second);
  }
}

bool StatHistogram::isReadyForWriting() const {
  // Histograms should only be written at the end of the run
  return false;
}
