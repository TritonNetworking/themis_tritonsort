#ifndef TRITONSORT_MAIN_UTILS_H
#define TRITONSORT_MAIN_UTILS_H

// Utility functions used by both graysort and minutesort main.cc files

#include <list>
#include <vector>
#include <map>
#include "core/constants.h"
#include "common/sort_constants.h"

class File;
class Params;
class WorkerTracker;

typedef std::map< uint64_t, std::list<File*> > ReaderIDToFileListMap;

// Log the time that the run started
void logStartTime();

// If parameter parameterName is defined, parse its value as a comma-delimited
// list of disk paths. Otherwise, assert that a parameter "$parameterName_FILE"
// is defined and treat its value as a file containing a newline-delimited list
// of disks.
void getDiskList(StringList& diskList, const std::string& parameterName,
                 const Params* params);

// Dump stack, print segfault and raise sigterm
void sigsegvHandler(int signal=0);

#endif // TRITONSORT_MAIN_UTILS_H
