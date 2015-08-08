#include <limits>

#include "mapreduce/common/StreamInfo.h"

StreamInfo::StreamInfo(uint64_t _streamID, const std::string& _filename)
  : streamID(_streamID),
    filename(_filename),
    size(std::numeric_limits<uint64_t>::max()) {
}

StreamInfo::StreamInfo(
  uint64_t _streamID, const std::string& _filename, uint64_t _size)
  : streamID(_streamID),
    filename(_filename),
    size(_size) {
}

void StreamInfo::addJobID(uint64_t jobID) {
  jobIDs.insert(jobID);
}

uint64_t StreamInfo::getStreamID() const {
  return streamID;
}

const std::set<uint64_t>& StreamInfo::getJobIDs() const {
  return jobIDs;
}

const std::string& StreamInfo::getFilename() const {
  return filename;
}

uint64_t StreamInfo::getSize() const {
  return size;
}
