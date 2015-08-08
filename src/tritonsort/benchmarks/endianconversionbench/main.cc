#define _BSD_SOURCE
#include <endian.h>

#include <iostream>

#include "core/ByteOrder.h"
#include "core/Timer.h"

int main(int argc, char** argv) {
  // Run 1 million conversions of DEADBEEF + i from host to big endian.
  uint64_t deadbeef = 0xDEADBEEF;
  uint64_t N = 1000000;
  uint64_t output = 0;
  Timer timer;

  // htobe64()
  timer.start();
  for (uint64_t i = 0; i < N; ++i) {
    output = htobe64(deadbeef + i);
  }
  timer.stop();
  std::cout << "htobe64, " << N << " iterations: " << timer.getElapsed()
            << " us" << std::endl;

  // hostToBigEndian64()
  timer.start();
  for (uint64_t i = 0; i < N; ++i) {
    output = hostToBigEndian64(deadbeef + i);
  }
  timer.stop();
  std::cout << "hostToBigEndian64, " << N << " iterations: "
            << timer.getElapsed() << " us" << std::endl;

  // So g++ won't complain about unused variables.
  std::cout << output << std::endl;
}
