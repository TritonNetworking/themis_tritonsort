#include "UInt64Resource.h"

UInt64Resource::UInt64Resource(uint64_t _number)
: number(_number) {

}

uint64_t UInt64Resource::asUInt64() const {
  return number;
}


uint64_t UInt64Resource::getCurrentSize() const {
  return sizeof(number);
}
