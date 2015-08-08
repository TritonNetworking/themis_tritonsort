#include <stdlib.h>

#include "common/AlignmentUtils.h"

uint64_t roundUp(uint64_t size, uint64_t memAlignment) {
  if (memAlignment == 0) {
    return size;
  }
  return (((size + memAlignment - 1) / memAlignment)) * memAlignment;
}

uint8_t* align(uint8_t* buf, uint64_t memAlignment) {
  if (buf == NULL) {
    return NULL;
  }

  return reinterpret_cast<uint8_t*>(
    roundUp(reinterpret_cast<uint64_t>(buf), memAlignment));
}
