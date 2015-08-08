#define __STDC_FORMAT_MACROS 1

#include <inttypes.h>
#include <new>

#include "core/LogLineDescriptor.h"
#include "core/LoggableUInt64Datum.h"
#include "core/TritonSortAssert.h"

LoggableUInt64Datum::LoggableUInt64Datum(uint64_t _datum) : datum(_datum) {
}

void LoggableUInt64Datum::write(
  int fileDescriptor, const std::string& phaseName, uint64_t epoch,
  const LogLineDescriptor& descriptor) {
  descriptor.writeLogLineToFile(fileDescriptor, phaseName.c_str(), epoch,
                                datum);
}

LogLineDescriptor* LoggableUInt64Datum::createLogLineDescriptor(
  const LogLineDescriptor& parentDescriptor, const std::string& statName)
  const {

  LogLineDescriptor* statDescriptor = new (std::nothrow)
    LogLineDescriptor(parentDescriptor);
  ABORT_IF(statDescriptor == NULL,
           "Ran out of memory while allocating LogLineDescriptor");

  statDescriptor->addConstantStringField("stat_name", statName)
    .addField("uint_value", LogLineFieldInfo::UNSIGNED_INTEGER)
    .finalize();

  return statDescriptor;
}
