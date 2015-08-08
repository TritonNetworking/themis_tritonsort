#ifndef THEMIS_TEST_AUTO_UNLINK_FILE_WRAPPER_H
#define THEMIS_TEST_AUTO_UNLINK_FILE_WRAPPER_H

#include "core/File.h"

/**
   Automatically unlinks the file when it goes out of scope
 */
class AutoUnlinkFileWrapper {
public:
  AutoUnlinkFileWrapper(const std::string& filename);
  virtual ~AutoUnlinkFileWrapper();

  File file;
};


#endif // THEMIS_TEST_AUTO_UNLINK_FILE_WRAPPER_H
