#include "tests/themis_core/StringWorkUnit.h"

StringWorkUnit::StringWorkUnit(const std::string& _str)
  : str(_str) {
}

uint64_t StringWorkUnit::getCurrentSize() const {
  return str.size();
}

const std::string& StringWorkUnit::getString() const {
  return str;
}
