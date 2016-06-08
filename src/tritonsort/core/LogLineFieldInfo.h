#ifndef TRITONSORT_LOG_LINE_FIELD_INFO_H
#define TRITONSORT_LOG_LINE_FIELD_INFO_H

#include <json/json.h>"
#include <stdint.h>
#include <string>

/**
   LogLineFieldInfo stores information about a field in a log line. It is used
   by LogLineDescriptor as an encapsulated representation of the properties of
   its fields.

   \TODO(AR) currently whether or not the field is declared constant is not
   output to the stat description files to maintain compatibility with the
   existing metaprogram infrastructure.
 */
class LogLineFieldInfo {
public:
  /// Describes the types that a log line field's value can assume
  enum FieldType {
    UNSIGNED_INTEGER, /// Unsigned integer
    SIGNED_INTEGER, /// Signed integer
    STRING /// String
  };

  /// Copy Constructor
  /**
     \param other the LogLineFieldInfo to copy
   */
  LogLineFieldInfo(const LogLineFieldInfo& other);

  /// Construct a LogLineFieldInfo for a variable field
  /**
     \param fieldName the name of the field
     \param type the field's data type

     \return a new LogLineFieldInfo corresponding to the field
   */
  static LogLineFieldInfo* newVariableField(
    const std::string& fieldName, FieldType type);

  /// Construct a LogLineFieldInfo for a constant unsigned integer field
  /**
     \param fieldName the name of the field

     \param value the field's constant value

     \return a new LogLineFieldInfo corresponding to the field
   */
  static LogLineFieldInfo* newConstantUIntField(
    const std::string& fieldName, uint64_t value);

  /// Construct a LogLineFieldInfo for a constant signed integer field
  /**
     \param fieldName the name of the field

     \param value the field's constant value

     \return a new LogLineFieldInfo corresponding to the field
   */
  static LogLineFieldInfo* newConstantIntField(
    const std::string& fieldName, int64_t value);

  /// Construct a LogLineFieldInfo for a constant string field
  /**
     \param fieldName the name of the field

     \param value the field's constant value

     \return a new LogLineFieldInfo corresponding to the field
   */
  static LogLineFieldInfo* newConstantStringField(
    const std::string& fieldName, const std::string& value);

  /// Get the format string for this field
  /**
     \return a printf-style format string corresponding to this field
   */
  const std::string& getFormatString() const;


  /// Return a JSON object describing this field
  /**
     \return a description string giving the field's name and its type
   */
  const Json::Value& getDescriptionJson() const;

private:
  std::string formatString;
  Json::Value description;


  LogLineFieldInfo(
    const std::string& fieldName, const std::string& valueFormatString,
    FieldType fieldType);

  LogLineFieldInfo(
    const std::string& fieldName, uint64_t constantValue);

  LogLineFieldInfo(
    const std::string& fieldName, int64_t constantValue);

  LogLineFieldInfo(
    const std::string& fieldName, const std::string& constantValue);
};

#endif // TRITONSORT_LOG_LINE_FIELD_INFO_H
