#define __STDC_FORMAT_MACROS

#include <boost/lexical_cast.hpp>
#include <inttypes.h>

#include "LogLineFieldInfo.h"
#include "TritonSortAssert.h"

LogLineFieldInfo::LogLineFieldInfo(const std::string& fieldName,
                                   const std::string& valueFormatString,
                                   FieldType fieldType)
  : formatString(valueFormatString),
    description(Json::objectValue) {
  description["name"] = fieldName;

  switch (fieldType) {
  case UNSIGNED_INTEGER:
    description["type"] = "uint";
    break;
  case SIGNED_INTEGER:
    description["type"] = "int";
    break;
  case STRING:
    description["type"] = "str";
    break;
  }
}

LogLineFieldInfo::LogLineFieldInfo(
  const std::string& fieldName, uint64_t constantValue)
  : formatString(boost::lexical_cast<std::string>(constantValue)),
    description(Json::objectValue) {
  description["name"] = fieldName;
  description["type"] = "uint";
}

LogLineFieldInfo::LogLineFieldInfo(
  const std::string& fieldName, int64_t constantValue)
  : formatString(boost::lexical_cast<std::string>(constantValue)),
    description(Json::objectValue) {

  description["name"] = fieldName;
  description["type"] = "int";
}

LogLineFieldInfo::LogLineFieldInfo(
  const std::string& fieldName, const std::string& constantValue)
  : formatString(constantValue),
    description(Json::objectValue) {

  description["name"] = fieldName;
  description["type"] = "str";
}


LogLineFieldInfo::LogLineFieldInfo(const LogLineFieldInfo& other)
  : formatString(other.formatString),
    description(other.description) {
}

LogLineFieldInfo* LogLineFieldInfo::newVariableField(
  const std::string& fieldName, FieldType type) {

  std::string formatString;

  switch (type) {
  case UNSIGNED_INTEGER:
    formatString.assign("%" PRIu64);
    break;
  case SIGNED_INTEGER:
    formatString.assign("%" PRId64);
    break;
  case STRING:
    formatString.assign("%s");
    break;
  default:
    ABORT("Unhandled field type %d", type);
    break;
  }

  return new LogLineFieldInfo(fieldName, formatString, type);
}

LogLineFieldInfo* LogLineFieldInfo::newConstantUIntField(
  const std::string& fieldName, uint64_t value) {

  return new LogLineFieldInfo(fieldName, value);
}

LogLineFieldInfo* LogLineFieldInfo::newConstantIntField(
  const std::string& fieldName, int64_t value) {

  return new LogLineFieldInfo(fieldName, value);
}

LogLineFieldInfo* LogLineFieldInfo::newConstantStringField(
  const std::string& fieldName, const std::string& value) {

  return new LogLineFieldInfo(fieldName, value);
}

const std::string& LogLineFieldInfo::getFormatString() const {
  return formatString;
}

const Json::Value& LogLineFieldInfo::getDescriptionJson() const {
  return description;
}
