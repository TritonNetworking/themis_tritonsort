#define __STDC_FORMAT_MACROS 1

#include <errno.h>
#include <sstream>
#include <stdio.h>
#include <string.h>

#include "core/LogLineDescriptor.h"
#include "core/TritonSortAssert.h"
#include "third-party/jsoncpp.h"

LogLineDescriptor::LogLineDescriptor()
  : description(Json::objectValue) {
  init();
}

LogLineDescriptor::LogLineDescriptor(const LogLineDescriptor& descriptor)
  : logLineTypeName(descriptor.logLineTypeName) {

  if (!descriptor.finalized) {
    for (FieldList::const_iterator iter =
           descriptor.fields.begin(); iter != descriptor.fields.end(); iter++) {
      fields.push_back(new LogLineFieldInfo(**iter));
    }

    init();
  } else {
    description = descriptor.description;
    formatString.assign(descriptor.formatString);
    finalized = true;
  }
}

LogLineDescriptor::~LogLineDescriptor() {
  for (FieldList::iterator iter = fields.begin(); iter != fields.end(); iter++) {
    delete *iter;
  }

  fields.clear();
}

LogLineDescriptor& LogLineDescriptor::setLogLineTypeName(
  const std::string& logLineTypeName) {
  this->logLineTypeName.assign(logLineTypeName);

  return *this;
}

void LogLineDescriptor::init() {
  finalized = false;
}

LogLineDescriptor& LogLineDescriptor::addField(
  const std::string& fieldName, LogLineFieldInfo::FieldType fieldType) {
  ABORT_IF(finalized, "Can't add a field to a finalized LogLineDescriptor");

  fields.push_back(LogLineFieldInfo::newVariableField(fieldName,
                                                      fieldType));

  return *this;
}

LogLineDescriptor& LogLineDescriptor::addConstantUIntField(
  const std::string& fieldName, uint64_t value) {
  ABORT_IF(finalized, "Can't add a field to a finalized LogLineDescriptor");

  fields.push_back(LogLineFieldInfo::newConstantUIntField(fieldName, value));

  return *this;
}

LogLineDescriptor& LogLineDescriptor::addConstantIntField(
  const std::string& fieldName, int64_t value) {
  ABORT_IF(finalized, "Can't add a field to a finalized LogLineDescriptor");

  fields.push_back(LogLineFieldInfo::newConstantIntField(fieldName, value));

  return *this;
}

LogLineDescriptor& LogLineDescriptor::addConstantStringField(
  const std::string& fieldName, const std::string& value) {
  ABORT_IF(finalized, "Can't add a field to a finalized LogLineDescriptor");

  fields.push_back(LogLineFieldInfo::newConstantStringField(fieldName, value));

  return *this;
}

void LogLineDescriptor::finalize() {
  uint64_t numFields = fields.size();

  ABORT_IF(numFields == 0, "There's no point in logging something with "
           "zero fields!");

  ABORT_IF(logLineTypeName.size() == 0, "Must specify a log line type "
           "prefix string for all log line descriptors");

  std::ostringstream formatStringStream;

  // The third field of every log line descriptor is the epoch number
  numFields++;
  fields.push_front(LogLineFieldInfo::newVariableField(
                      "epoch", LogLineFieldInfo::UNSIGNED_INTEGER));

  // The second field of every log line descriptor is the phase name
  numFields++;
  fields.push_front(LogLineFieldInfo::newVariableField(
                      "phase_name", LogLineFieldInfo::STRING));

  // The first field of every log line descriptor is its log line type name
  numFields++;
  fields.push_front(LogLineFieldInfo::newConstantStringField(
                      "line_type_name", logLineTypeName));

  description["type"] = logLineTypeName;
  description["fields"] = Json::Value(Json::arrayValue);

  Json::Value& descriptionFields = description["fields"];

  FieldList::iterator fieldIter = fields.begin();

  for (uint64_t fieldID = 0; fieldID < numFields; fieldID++) {
    LogLineFieldInfo* fieldInfo = *fieldIter;

    formatStringStream << fieldInfo->getFormatString();

    descriptionFields.append(fieldInfo->getDescriptionJson());

    if (fieldID != numFields - 1) {
      formatStringStream << '\t';
    }

    delete fieldInfo;
    fieldIter++;
  }

  fields.clear();

  formatStringStream << std::endl;
  formatString = formatStringStream.str();

  finalized = true;
}

void LogLineDescriptor::writeLogLineToFile(int fileDescriptor ...) const {
  ASSERT(finalized, "Can't write log lines for an unfinalized "
         "LogLineDescriptor");

  const char* format = formatString.c_str();

  va_list ap;
  va_start(ap, fileDescriptor);

  int status = vdprintf(fileDescriptor, format, ap);

  ABORT_IF(status < 0, "vdprintf() failed with error %d", status);

  va_end(ap);
}

const Json::Value& LogLineDescriptor::getDescriptionJson() const {
  ASSERT(finalized, "Descriptor must be finalized before returning "
         "description string");
  return description;
}

const std::string& LogLineDescriptor::getLogLineFormatString() const {
  ASSERT(finalized, "Descriptor must be finalized before returning "
         "log line format string");
  return formatString;
}
