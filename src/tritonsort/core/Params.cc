
#include <boost/filesystem.hpp>
#include <sstream>
#include <yaml-cpp/yaml.h>

#include "core/Params.h"
#include "core/StatusPrinter.h"

Params::Params() {
}

Params::~Params() {
  clear();
}

Params::ParamValue& Params::getValueContainer(const std::string& key) const {
  ParamValue* valuePtr = NULL;

  ParamValueMap::const_iterator iter = params.find(key);

  if (iter != params.end()) {
    valuePtr = iter->second;
  }

  ABORT_IF(valuePtr == NULL, "Parameter '%s' is required, but its value "
           "has not been specified.", key.c_str());

  return *valuePtr;
}

bool Params::contains(const std::string& key) const {
  ParamValueMap::const_iterator mapIter = params.find(key);

  return mapIter != params.end();
}

bool Params::containsv(const char* format, ...) const {
  va_list ap;
  va_start(ap, format);
  char* key;
  ABORT_IF(vasprintf(&key, format, ap) == -1, "vasprintf() failed while "
           "running Params::containsv");
  va_end(ap);

  bool containsKey = contains(key);

  free(key);

  return containsKey;
}

void Params::parseFile(const char* filename) {
  TRITONSORT_ASSERT(fileExists(filename), "Can't find params file '%s'", filename);

  YAML::Node doc = YAML::LoadFile(filename);

  // Iterate over the resulting YAML node, adding all parameters to the
  // internal parameter map
  parseYAMLNode(doc);
}

void Params::parseYAMLNode(const YAML::Node& node) {
  std::list<std::string> keyStack;
  parseYAMLNode(node, keyStack);

  TRITONSORT_ASSERT(keyStack.empty());
}

void Params::parseYAMLNode(
  const YAML::Node& node, std::list<std::string>& keyStack) {

  // Since we may have arbitrarily nested key/value pairs
  std::stack<std::string> keyChunks;

  std::ostringstream keyConcatStream;

  // Join all previous parts of the key with '.' to form a "prefix" for all
  // keys under this node
  for (std::list<std::string>::iterator keyStackIter = keyStack.begin();
       keyStackIter != keyStack.end(); keyStackIter++) {
    keyConcatStream << *keyStackIter << '.';
  }

  std::string keyPrefix(keyConcatStream.str());

  for (YAML::const_iterator iter = node.begin(); iter != node.end(); iter++) {
    const YAML::Node& key = iter->first;
    const YAML::Node& value = iter->second;

    ABORT_IF(key.Type() != YAML::NodeType::Scalar,
             "YAML config files are expected to have scalar keys");

    std::string keyStr = key.as<std::string>();
    std::string valueStr;

    switch (value.Type()) {
    case YAML::NodeType::Null:
      ABORT("Params doesn't currently support null values (key %s)",
            keyStr.c_str());
      break;
    case YAML::NodeType::Scalar:
      valueStr = value.as<std::string>();

      if (keyPrefix.size() > 0) {
        add(keyPrefix + keyStr, valueStr);
      } else {
        add(keyStr, valueStr);
      }

      break;
    case YAML::NodeType::Sequence:
      ABORT("Params doesn't currently support null values");
      break;
    case YAML::NodeType::Map:
      // Append the current key to the stack and recursively evaluate the map
      keyStack.push_back(keyStr);
      parseYAMLNode(value, keyStack);

      // Done dealing with the map, so pop the current key from the stack
      keyStack.pop_back();
      break;
    case YAML::NodeType::Undefined:
      ABORT("Undefined node type");
      break;
    }
  }
}

void Params::clear() {
  for (ParamValueMap::iterator iter = params.begin(); iter != params.end();
       iter++) {
    delete iter->second;
  }

  params.clear();
}

void Params::parseCommandLineArguments(int argc, char** argv) {
  int startIndex = 1;

  if ((argc - startIndex) % 2 != 0) {
    std::cerr << "Invalid argument structure (expecting "
              << "'program-name [params file] -param1 val1 "
              << "-param2 val2 ...')" << std::endl;
    exit(3);
  }

  int i = argc - 1;
  char* key = NULL;
  char* value = NULL;

  while (i > 1) {
    key = argv[i - 1];
    value = argv[i];

    if (key[0] == '-') {
      // Strip the leading '-' from the key
      std::string keyStr(key, 1, strlen(key) - 1);

      // If the value starts with a '-', check to see if it's a number
      if (value[0] == '-') {
        try {
          double valueAsDouble = boost::lexical_cast<double>(value);
          // Add value here to avoid unused variable warning
          add(keyStr, valueAsDouble);
        } catch (boost::bad_lexical_cast& exception) {
          // This value is probably the key for some other parameter
          ABORT("Possibly malformed parameter list; non-numeric value '%s' for "
                "key '%s' starts with a '-'", value, key);
        }
      } else {
        std::string valStr(value);
        add(keyStr, valStr);
      }

      i -= 2;
    } else {
      ABORT("Malformed parameter list; expect all keys to begin with '-', but "
            "key '%s' does not", key);
    }
  }
}

void Params::parseCommandLine(int argc, char* argv[]) {
  if (argc == 2) {
    parseFile(argv[1]);
  } else if (argc >= 2) {
    parseCommandLineArguments(argc, argv);

    // Load default config file if available.
    if (contains("DEFAULT_CONFIG")) {
      std::ostringstream oss;
      oss << "Reading from default config file "
          << get<std::string>("DEFAULT_CONFIG");

      StatusPrinter::add(oss.str());
      parseFile(get<std::string>("DEFAULT_CONFIG").c_str());
    }

    // Load application config file if available.
    if (contains("CONFIG")) {
      std::ostringstream oss;
      oss << "Reading from config file "
          << get<std::string>("CONFIG");

      StatusPrinter::add(oss.str());
      parseFile(get<std::string>("CONFIG").c_str());
    }

    // This is a bit inelegant, but is necessary to override any config
    // file options with the command line.
    parseCommandLineArguments(argc, argv);
  }
}

void Params::dump(const std::string& filename) {
  std::ofstream ofs;
  ofs.open(filename.c_str());

  for (ParamValueMap::iterator iter = params.begin(); iter != params.end();
       iter++) {
    ofs << iter->first << ": ";
    iter->second->print(ofs);
    ofs << std::endl;
  }

  ofs.close();

  ABORT_IF(ofs.fail(), "Writing to params file %s failed", filename.c_str());
}

Params::ParamValue::ParamValue(const std::string& value)
  : strValue(value) {
}

Params::ParamValue::ParamValue(bool value) {
  setStringValue<bool>(value);
}

Params::ParamValue::ParamValue(int32_t value) {
  setStringValue<int32_t>(value);
}

Params::ParamValue::ParamValue(uint32_t value) {
  setStringValue<uint32_t>(value);
}

Params::ParamValue::ParamValue(int64_t value) {
  setStringValue<int64_t>(value);
}

Params::ParamValue::ParamValue(uint64_t value) {
  setStringValue<uint64_t>(value);
}

Params::ParamValue::ParamValue(double value) {
  setStringValue<double>(value);
}


Params::ParamValue::~ParamValue() {
}

bool Params::ParamValue::get(bool& value) const {
  if (strValue == "true" || strValue == "1") {
    value = true;
  } else if (strValue == "false" || strValue == "0") {
    value = false;
  } else {
    return false;
  }

  return true;
}

bool Params::ParamValue::get(int32_t& value) const {
  return getNumericValue<int32_t>(value);
}

bool Params::ParamValue::get(uint32_t& value) const {
  return getNumericValue<uint32_t>(value);
}

bool Params::ParamValue::get(int64_t& value) const {
  return getNumericValue<int64_t>(value);
}

bool Params::ParamValue::get(uint64_t& value) const {
  return getNumericValue<uint64_t>(value);
}

bool Params::ParamValue::get(double& value) const {
  return getNumericValue<double>(value);
}

bool Params::ParamValue::get(std::string& value) const {
  value.assign(strValue);
  return true;
}

bool Params::ParamValue::isNumeric() const {
  double doubleValue = 0;
  int64_t signedValue = 0;
  uint64_t unsignedValue = 0;

  return get(doubleValue) || get(unsignedValue) || get(signedValue);
}

void Params::ParamValue::print(std::ostream& stream) const {
  if (isNumeric()) {
    stream << strValue;
  } else {
    stream << "\"" << strValue << "\"";
  }
}
