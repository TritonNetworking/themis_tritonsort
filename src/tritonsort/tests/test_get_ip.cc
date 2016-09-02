#include <iostream>

#include "core/TritonSortAssert.h"
#include "core/Utils.h"

int main(int argc, char** argv) {
  TRITONSORT_ASSERT(argc == 2);

  std::string interface(argv[1]);

  std::cout << "IP address for this host on interface " << interface << ":";

  std::string* hostIPString = getIPAddress(interface);

  std::cout << *hostIPString << std::endl;

  delete hostIPString;

  return 0;
}
