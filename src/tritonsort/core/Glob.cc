#include <errno.h>
#include <glob.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "core/File.h"
#include "core/Glob.h"
#include "core/TritonSortAssert.h"

Glob::Glob(const std::string& globPattern) {
  globFiles(globPattern);
}

Glob::Glob(const StringList& globPatternList) {
  for (StringList::const_iterator iter = globPatternList.begin();
       iter != globPatternList.end(); iter++) {
    globFiles(*iter);
  }
}

Glob::~Glob() {
  files.clear();
  directories.clear();
}

void Glob::globFiles(const std::string& globPattern) {
  int flags = GLOB_TILDE_CHECK;

  glob_t globResults;

  int status = glob(globPattern.c_str(), flags, NULL, &globResults);

  if (status != 0) {
    globfree(&globResults);

    switch (status) {
    case GLOB_NOSPACE:
      ABORT("Ran out of space while globbing with pattern '%s'",
            globPattern.c_str());
      break;
    case GLOB_ABORTED:
      ABORT("Encountered a read error while globbing with pattern '%s'",
            globPattern.c_str());
      break;
    case GLOB_NOMATCH:
      // No matches to the glob; just don't add any files
      break;
    default:
      ABORT("Should have aborted in globErrorFunction if "
            "return code from glob() was non-zero, but we didn't "
            "(status is %d: %s)", status, strerror(status));
      break;
    }
  }

  if (status == 0) {
    struct stat currentStat;

    for (uint64_t i = 0; i < globResults.gl_pathc; i++) {
      const char* match = globResults.gl_pathv[i];
      int status = stat(match, &currentStat);

      ABORT_IF(status == -1, "stat(%s) failed with error %d: %s", match,
               errno, strerror(errno));

      if (S_ISREG(currentStat.st_mode)) {
        files.push_back(match);
      } else if (S_ISDIR(currentStat.st_mode)) {
        directories.push_back(match);
      }
    }
  }

  globfree(&globResults);
}

const StringList& Glob::getFiles() const {
  return files;
}

const StringList& Glob::getDirectories() const {
  return directories;
}
