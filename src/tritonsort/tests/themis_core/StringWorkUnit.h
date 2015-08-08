#ifndef THEMIS_TEST_STRING_WORK_UNIT_H
#define THEMIS_TEST_STRING_WORK_UNIT_H

#include <stdint.h>
#include <string>

#include "core/Resource.h"

class StringWorkUnit : public Resource {
public:
  StringWorkUnit(const std::string& str);

  uint64_t getCurrentSize() const;

  const std::string& getString() const;
private:
  std::string str;
};

#endif // THEMIS_TEST_STRING_WORK_UNIT_H
