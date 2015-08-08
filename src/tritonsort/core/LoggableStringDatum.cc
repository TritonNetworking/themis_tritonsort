#include <new>

#include "LogLineDescriptor.h"
#include "LoggableStringDatum.h"

LoggableStringDatum::LoggableStringDatum(const std::string& _datum)
  : datum(_datum) {
}

void LoggableStringDatum::write(int fileDescriptor,
                                const std::string& phaseName,
                                uint64_t epoch,
                                const LogLineDescriptor& descriptor) {
  descriptor.writeLogLineToFile(fileDescriptor, phaseName.c_str(), epoch,
                                datum.c_str());
}

LogLineDescriptor* LoggableStringDatum::createLogLineDescriptor(
  const LogLineDescriptor& parentDescriptor, const std::string& statName)
  const {

  LogLineDescriptor* statDescriptor = new (std::nothrow) LogLineDescriptor(
    parentDescriptor);
  ABORT_IF(statDescriptor == NULL,
           "Ran out of memory while allocating LogLineDescriptor");

  statDescriptor->addConstantStringField("stat_name", statName)
    .addField("str_value", LogLineFieldInfo::STRING)
    .finalize();

  return statDescriptor;
}
