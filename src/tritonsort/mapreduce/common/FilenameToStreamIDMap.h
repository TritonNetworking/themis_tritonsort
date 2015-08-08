#ifndef MAPRED_FILENAME_TO_STREAM_ID_MAP_H
#define MAPRED_FILENAME_TO_STREAM_ID_MAP_H

#include <map>
#include <pthread.h>
#include <stdint.h>
#include <string>
#include <set>

#include "core/constants.h"

class StreamInfo;

/**
   Used by the Reader and ByteStreamConverter workers as a bidirectional
   mapping between filenames and stream IDs.
 */
class FilenameToStreamIDMap {
public:
  /// Constructor
  FilenameToStreamIDMap();

  /// Destructor
  virtual ~FilenameToStreamIDMap();

  /// Add a filename to the mapping
  /**
     As filenames are added to the mapping with this method, they are assigned
     unique, monotonically increasing stream IDs.

     Thread-safe.

     \warning Aborts if the same filename is added to the map multiple times

     \param filename the filename to add to the mapping

     \param jobIDs the set of jobs for which data from this file should be
     processed

     \param offset the offset into the file
   */
  void addFilename(
    const std::string& filename, const std::set<uint64_t>& jobIDs,
    uint64_t offset=0);

  /// Add a filename to the mapping with a specified size that instructs the
  /// stream converter to allocate a single buffer to hold the entire file.
  /**
     As filenames are added to the mapping with this method, they are assigned
     unique, monotonically increasing stream IDs.

     Thread-safe.

     \warning Aborts if the same filename is added to the map multiple times

     \param filename the filename to add to the mapping

     \param jobIDs the set of jobs for which data from this file should be
     processed

     \param size the size of the file

     \param offset the offset into the file
   */
  void addFilenameWithSize(
    const std::string& filename, const std::set<uint64_t>& jobIDs,
    uint64_t size, uint64_t offset=0);

  /// Get information about a stream
  /**
     \param streamID the stream ID of the stream

     \return a reference to the StreamInfo object containing information about
     the stream
   */
  const StreamInfo& getStreamInfo(uint64_t streamID);

  /// Get information about a stream
  /**
     \param filename the filename whose stream information is requested

     \param offset the offset into the file

     \return a reference to the StreamInfo object containing information about
     the stream
   */
  const StreamInfo& getStreamInfo(
    const std::string& filename, uint64_t offset=0);
private:
  DISALLOW_COPY_AND_ASSIGN(FilenameToStreamIDMap);

  typedef std::map<std::string, StreamInfo*> FilenameToStreamMap;
  typedef std::map<uint64_t, StreamInfo*> StreamIDToStreamInfoMap;

  pthread_mutex_t lock;
  uint64_t lastStreamID;
  FilenameToStreamMap fileToStreamMap;
  StreamIDToStreamInfoMap streamIDToStreamInfoMap;
};

#endif // MAPRED_FILENAME_TO_STREAM_ID_MAP_H
