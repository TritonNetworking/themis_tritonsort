#include <limits>
#include <sstream>

#include "FilenameToStreamIDMap.h"
#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/StreamInfo.h"

FilenameToStreamIDMap::FilenameToStreamIDMap() {
  lastStreamID = 0;
  pthread_mutex_init(&lock, NULL);
}

FilenameToStreamIDMap::~FilenameToStreamIDMap() {
  pthread_mutex_lock(&lock);

  for (StreamIDToStreamInfoMap::iterator iter = streamIDToStreamInfoMap.begin();
       iter != streamIDToStreamInfoMap.end(); iter++) {
    delete iter->second;
  }

  fileToStreamMap.clear();
  streamIDToStreamInfoMap.clear();

  pthread_mutex_unlock(&lock);

  pthread_mutex_destroy(&lock);
}

void FilenameToStreamIDMap::addFilename(
  const std::string& filename, const std::set<uint64_t>& jobIDs,
  uint64_t offset) {
  // Use uint64_t max as the size to inform the byte stream converter that we
  // don't care about the size of the stream
  addFilenameWithSize(
    filename, jobIDs, std::numeric_limits<uint64_t>::max(), offset);
}

void FilenameToStreamIDMap::addFilenameWithSize(
  const std::string& filename, const std::set<uint64_t>& jobIDs,
  uint64_t size, uint64_t offset) {

  std::ostringstream oss;
  oss << filename;
  if (offset > 0) {
    // Read requests from different offsets are technically different streams.
    oss << ".offset_" << offset;
  }
  std::string fullFilename = oss.str();

  ScopedLock scopedLock(&lock);

  FilenameToStreamMap::iterator iter = fileToStreamMap.find(fullFilename);

  ABORT_IF(iter != fileToStreamMap.end(), "Can't add the same filename "
           "('%s') to the mapping multiple times", fullFilename.c_str());

  uint64_t streamID = lastStreamID++;
  StreamInfo* streamInfo = new StreamInfo(streamID, fullFilename, size);

  for (std::set<uint64_t>::iterator iter = jobIDs.begin(); iter != jobIDs.end();
       iter++) {
    streamInfo->addJobID(*iter);
  }

  fileToStreamMap[fullFilename] = streamInfo;
  streamIDToStreamInfoMap[streamID] = streamInfo;
}

const StreamInfo& FilenameToStreamIDMap::getStreamInfo(uint64_t streamID) {
  ScopedLock scopedLock(&lock);

  StreamIDToStreamInfoMap::iterator iter = streamIDToStreamInfoMap.find(
    streamID);

  ABORT_IF(iter == streamIDToStreamInfoMap.end(), "Can't find stream info for "
           "stream ID %llu", streamID);

  return *(iter->second);
}

const StreamInfo& FilenameToStreamIDMap::getStreamInfo(
  const std::string& filename, uint64_t offset) {

  std::ostringstream oss;
  oss << filename;
  if (offset > 0) {
    // Read requests from different offsets are technically different streams.
    oss << ".offset_" << offset;
  }
  std::string fullFilename = oss.str();

  ScopedLock scopedLock(&lock);

  FilenameToStreamMap::iterator iter = fileToStreamMap.find(
    fullFilename);

  ABORT_IF(iter == fileToStreamMap.end(), "Can't find stream info for "
           "filename '%s'", fullFilename.c_str());

  return *(iter->second);
}
