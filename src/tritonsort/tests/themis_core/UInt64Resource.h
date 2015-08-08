#ifndef THEMIS_UINT_64_RESOURCE_H
#define THEMIS_UINT_64_RESOURCE_H

#include <stdint.h>

#include "core/Resource.h"

class UInt64Resource : public Resource {
public:
  explicit UInt64Resource(uint64_t number);

  uint64_t asUInt64() const;

  uint64_t getCurrentSize() const;
private:
  const uint64_t number;
};

#endif // THEMIS_UINT_64_RESOURCE_H
