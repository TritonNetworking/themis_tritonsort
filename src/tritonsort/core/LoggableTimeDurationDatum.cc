#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>
#include <new>

#include "core/LogLineDescriptor.h"
#include "core/LoggableTimeDurationDatum.h"
#include "core/TritonSortAssert.h"

LoggableTimeDurationDatum::LoggableTimeDurationDatum(const Timer& timer)
  : startTime(timer.getStartTime()),
    stopTime(timer.getStopTime()),
    elapsedTime(timer.getElapsed()) {
}

void LoggableTimeDurationDatum::write(
  int fileDescriptor, const std::string& phaseName, uint64_t epoch,
  const LogLineDescriptor& descriptor) {

  descriptor.writeLogLineToFile(fileDescriptor, phaseName.c_str(), epoch,
                                startTime, stopTime, elapsedTime);
}

LogLineDescriptor* LoggableTimeDurationDatum::createLogLineDescriptor(
  const LogLineDescriptor& parentDescriptor, const std::string& statName)
  const {

  LogLineDescriptor* statDescriptor = new (std::nothrow)
    LogLineDescriptor(parentDescriptor);
  ABORT_IF(statDescriptor == NULL,
           "Ran out of memory while allocating LogLineDescriptor");

  statDescriptor->addConstantStringField("stat_name", statName)
    .addField("start_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("stop_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .addField("elapsed_time", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();

  return statDescriptor;
}
