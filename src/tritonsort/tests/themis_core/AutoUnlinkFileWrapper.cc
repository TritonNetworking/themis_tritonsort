#include "tests/themis_core/AutoUnlinkFileWrapper.h"

AutoUnlinkFileWrapper::AutoUnlinkFileWrapper(const std::string& filename)
  : file(filename) {
}

AutoUnlinkFileWrapper::~AutoUnlinkFileWrapper() {
  if (file.isOpened()) {
    file.close();
  }
  file.unlink();
}
