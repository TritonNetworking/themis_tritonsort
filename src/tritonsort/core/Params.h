#ifndef _TRITONSORT_PARAMS_H
#define _TRITONSORT_PARAMS_H

#include <boost/lexical_cast.hpp>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <set>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <typeinfo>

#include "TritonSortAssert.h"
#include "Utils.h"
#include "constants.h"
#include "core/TritonSortAssert.h"

namespace YAML {
  class Node;
};

/**
   The Params object allows easy manipulation of and access to configuration
   parameters.
 */

class Params {
public:
  /// Constructor
  Params();

  /// Destructor
  virtual ~Params();

  /**
     Dumps all parameters. A parameter will only be dumped if it has been
     referenced (with Params::get) at some point during the program's execution.

     If test mode is enabled (see Params::enableTestMode) params are dumped to
     a file given by the LOG_FILE parameter. Otherwise, they're dumped to the
     StatusPrinter.

     \param filename the name of the file to which to dump params
   */
  void dump(const std::string& filename);

  /// Get the parameter value associated with the given key
  /**
     \param the key whose value is to be retrieved

     \return a copy of that parameter's value
   */
  template <typename T> T get(const std::string& key) const {
    ParamValue& value = getValueContainer(key);

    T valueToReturn;
    ABORT_IF(!value.get(valueToReturn), "Can't convert value for key '%s' "
             "to requested type", key.c_str());

    return valueToReturn;
  }

  template <typename T> T getv(const char* format, ...) const {
    va_list ap;
    va_start(ap, format);
    char* key;
    ABORT_IF(vasprintf(&key, format, ap) == -1, "vasprintf() failed "
             "while running Params::getv");
    va_end(ap);

    T valueToReturn = get<T>(key);

    free(key);

    return valueToReturn;
  }

  /// Is the given parameter defined?
  /**
     \param key the parameter whose existence is to be checked

     \return true if the parameter has a value, false otherwise
   */
  bool contains(const std::string& key) const;

  bool containsv(const char* format, ...) const;

  /// (Re-)assign the value of a parameter
  /**
     \warning Any previous definition of the given parameter will be deleted

     \param key the parameter key to be (re-)assigned
     \param value the parameter's value
   */
  template <typename T> void add(const std::string& key, const T& value) {
    ParamValueMap::iterator iter = params.find(key);

    if (iter != params.end()) {
      delete iter->second;
      params.erase(iter);
    }

    params.insert(std::make_pair(key, new ParamValue(value)));
  }

  /// Load parameters from the given configuration file
  /**
     The given configuration file is assumed to be written in YAML. Any
     existing parameter definitions will be overwritten by definitions of the
     same parameters in the provided file.

     \param filename the name of the file to load
   */
  void parseFile(const char* filename);

  /// Load parameters from command-line arguments.
  /**
     Params expects a command-line argument list of the following form:

     program-name [params file] -param1 val1 -param2 val2 ...

     If a params file is specified, it is loaded. Otherwise, all parameter
     names and values are parsed and inserted into this Params object as
     strings.

     \param argc the number of arguments to main
     \param argv the array of arguments to main
   */
  void parseCommandLine(int argc, char* argv[]);

  /// Delete all parameter definitions
  /**
     Delete (and free memory associated with) all parameter definitions
   */
  void clear();

private:
  /**
     Defines the various different equivalent representations of a parameter's
     value, and performs casting between these different types as necessary.
   */
  class ParamValue {
  public:
    /// Constructor
    /**
       \param value a possible representation of the parameter's value
     */
    ParamValue(const std::string& value);
    ParamValue(bool value);
    ParamValue(int32_t value);
    ParamValue(uint32_t value);
    ParamValue(int64_t value);
    ParamValue(uint64_t value);
    ParamValue(double value);

    /// Destructor
    virtual ~ParamValue();

    bool get(bool& value) const;
    bool get(int32_t& value) const;
    bool get(uint32_t& value) const;
    bool get(int64_t& value) const;
    bool get(uint64_t& value) const;
    bool get(double& value) const;
    bool get(std::string& value) const;

    /// Print a YAML-friendly string representation of this value
    /**
       If the value is a numeric type (see ParamValue::isNumeric), the value
       will be printed as normal; otherwise the value will be printed enclosed
       in quotes so that YAML interprets it as a string.

       \param stream the std::ostream to which the printed value will be written
     */
    void print(std::ostream& stream) const;

    /// Check if this value can be represented as a numeric type
    /**
       Attempt to represent value as a double, an unsigned integer and a signed
       integer; if that fails, assume the parameter is non-numeric

       \return true if this value can be represented as a numeric type, false
       otherwise
     */
    bool isNumeric() const;
  private:
    std::string strValue;

    template <typename T> void setStringValue(T value) {
      try {
        strValue.assign(boost::lexical_cast<std::string>(value));
      } catch (boost::bad_lexical_cast& exception) {
        ABORT("Can't cast provided value to a string");
      }
    }

    template <typename T> bool getNumericValue(T& value) const {
      const char* strValueCStr = strValue.c_str();

      // Special-case for hex, since lexical_cast doesn't handle it
      if (strValue.size() > 2 && strValueCStr[0] == '0' &&
          strValueCStr[1] == 'x') {
        std::istringstream iss(strValueCStr + 2);
        iss >> std::hex >> value;

        if ((iss.rdstate() & std::istringstream::failbit) != 0) {
          return false;
        }
      } else {
        try {
          value = boost::lexical_cast<T>(strValue);
        } catch (boost::bad_lexical_cast& exception) {
          return false;
        }
      }

      return true;
    }
  };

  typedef std::map<std::string, ParamValue*> ParamValueMap;

  ParamValueMap params;

  /// Retrieves the ParamValue container for the given parameter
  /**
     \param key the parameter to retrieve
     \return a reference to the ParamValue containing the parameter's value
   */
  ParamValue& getValueContainer(const std::string& key) const;

  void parseCommandLineArguments(int argc, char** argv);

  void parseYAMLNode(const YAML::Node& node);
  void parseYAMLNode(const YAML::Node& node, std::list<std::string>& keyStack);
};

#endif //_TRITONSORT_PARAMS_H
