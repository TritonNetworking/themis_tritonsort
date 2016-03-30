#include <algorithm>
#include <functional>
#include <iostream>
#include <signal.h>
#include <math.h>

#include "MainUtils.h"
#include "core/File.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "core/WorkerTracker.h"

void logStartTime() {
  time_t rawTime;
  char timeString[80];

  time(&rawTime);
  struct tm* timeInfo = localtime(&rawTime);
  strftime(timeString, 80, "%c", timeInfo);

  StatusPrinter::add("Started %s", timeString);
}

void getDiskList(StringList& diskList, const std::string& parameterName,
                 const Params* params) {
  if (params->contains(parameterName)) {
    const std::string& diskListString = params->get<std::string>(
      parameterName);
    parseCommaDelimitedList<std::string, StringList>(diskList, diskListString);
  } else {
    std::string fileParamName(parameterName);
    fileParamName += "_FILE";

    const std::string& diskListFile = params->get<std::string>(
      fileParamName);
    ASSERT(fileExists(diskListFile), "Can't find disk list file");
    parseDiskList(diskList, diskListFile);
  }
}

void sigsegvHandler(int signal_) {
  std::cerr << "Caught SIGSEGV. Segmentation Fault." << std::endl;
  // Reset the SIGSEGV handler so we can generate a core dump.
  signal(SIGSEGV, SIG_DFL);
}
