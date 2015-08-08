#ifndef TRITONSORT_LOG_LINE_DESCRIPTOR_H
#define TRITONSORT_LOG_LINE_DESCRIPTOR_H

#include <inttypes.h>
#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "LogLineFieldInfo.h"

/**
   LogLineDescriptors are used by stat containers (see StatContainerInterface)
   to describe the log lines they write to stat log files.
 */
class LogLineDescriptor {
public:

  /// Constructor
  LogLineDescriptor();

  /// Copy constructor
  /**
     \param descriptor the LogLineDescriptor being copied
   */
  LogLineDescriptor(const LogLineDescriptor& descriptor);

  /// Destructor
  virtual ~LogLineDescriptor();

  /// Set the log line type name for this descriptor
  /**
     The log line type name is used to easily differentiate between different
     kinds of log lines. Convention is usually that it is an all-caps,
     four-letter string like DATM or SUMM, but this is not strictly required.

     \param logLineTypeName the log line type name
   */
  LogLineDescriptor& setLogLineTypeName(const std::string& logLineTypeName);

  /// Add a field to the description
  /**
     This field will be added after all preceding fields declared with calls to
     addField()

     \param fieldName the name of the field

     \param fieldType a string declaring the type of the field. Will be used by
     the log processing scripts to automatically convert the field to the
     appropriate type

     \return a reference to this LogLineDescriptor
   */
  LogLineDescriptor& addField(const std::string& fieldName,
                              LogLineFieldInfo::FieldType fieldType
                              = LogLineFieldInfo::STRING);

  /// Add a constant string field to the log line descriptor
  /**
     This field will come after all previously added fields.

     \param fieldName the name of the field
     \param the field's constant value
   */
  LogLineDescriptor& addConstantStringField(const std::string& fieldName,
                                            const std::string& value);

  /// Add a constant unsigned integer field to the log line descriptor
  /**
     This field will come after all previously added fields.

     \param fieldName the name of the field
     \param the field's constant value
   */
  LogLineDescriptor& addConstantUIntField(const std::string& fieldName,
                                          uint64_t value);

  /// Add a constant signed integer field to the log line descriptor
  /**
     This field will come after all previously added fields.

     \param fieldName the name of the field
     \param the field's constant value
   */
  LogLineDescriptor& addConstantIntField(const std::string& fieldName,
                                         int64_t value);

  /// Indicate that you are done specifying the log line descriptor
  /**
     This method must be called before any methods other than field addition
     are used. Constructs the description string and the log line format string
     as side-effects.
   */
  void finalize();

  /// Get a JSON object describing this log line
  /**
     This JSON object is used by meta-programs to map field numbers to logical
     names.

     \return a reference to the JSON object that describes this log line
   */
  const Json::Value& getDescriptionJson() const;

  /// Get the log line format string for this log line
  /**
     The log line format string is a printf-style format string that will be
     combined with the descriptor's variable fields to produce the log line.

     \return a printf-style format string for use when printing this log line
   */
  const std::string& getLogLineFormatString() const;

  /// Write this log line to a file
  /**
     \param fileDescriptor an open file descriptor referring to the file to
     which to write

     \param ... the variable arguments for this log line, in order of their
     declaration
   */
  void writeLogLineToFile(int fileDescriptor ...) const;
private:
  typedef std::list<LogLineFieldInfo*> FieldList;

  std::string logLineTypeName;
  std::string formatString;
  Json::Value description;

  bool finalized;

  FieldList fields;

  void init();
};

#endif // TRITONSORT_LOG_LINE_DESCRIPTOR_H
