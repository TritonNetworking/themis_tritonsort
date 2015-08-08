#include "TestUtils.h"

uint8_t* makeTuple(uint8_t value, uint64_t tupleSize) {
  uint8_t* tuple = new uint8_t[tupleSize];

  for (uint64_t i = 0; i < tupleSize; i++) {
    tuple[i] = value;
  }

  return tuple;
}

uint8_t* makeTupleEx(uint8_t key0, uint8_t value, uint64_t tupleSize) {
  uint8_t* tuple = new uint8_t[tupleSize];

  tuple[0] = key0;
  for (uint64_t i = 1; i < tupleSize; i++) {
    tuple[i] = value;
  }

  return tuple;
}
