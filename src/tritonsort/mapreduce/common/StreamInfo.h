#ifndef THEMIS_MAPRED_STREAM_INFO_H
#define THEMIS_MAPRED_STREAM_INFO_H

#include <set>

#include "core/constants.h"

/**
   StreamInfo contains information about a stream of data being converted by a
   ByteStreamConverter, including that stream's filename, and the set of job
   IDs that should process data from the stream. This information is used by
   byte stream converters to attach the appropriate metadata to the buffers
   that they emit.
 */
class StreamInfo {
public:
  /// Constructor
  /**
     \param streamID the stream ID for this stream
     \param filename the filename from which this stream's data originates
   */
  explicit StreamInfo(uint64_t streamID, const std::string& filename);

  /// Constructor
  /**
     \param streamID the stream ID for this stream
     \param filename the filename from which this stream's data originates
     \param size the size of the stream if specified by the user
   */
  explicit StreamInfo(
    uint64_t streamID, const std::string& filename, uint64_t size);

  /// Destructor
  virtual ~StreamInfo() {}

  /// Associate a job ID with this stream
  /**
     \param jobID the job ID to associate with this stream
   */
  void addJobID(uint64_t jobID);

  /// \return the stream ID for this stream
  uint64_t getStreamID() const;

  /// \return the set of job IDs that should consume this stream's data
  const std::set<uint64_t>& getJobIDs() const;

  /// \return the filename from which this stream's data originates
  const std::string& getFilename() const;

  /// \return the size of the stream if set, or uint64_t max otherwise
  uint64_t getSize() const;

private:
  const uint64_t streamID;
  std::set<uint64_t> jobIDs;
  const std::string filename;
  const uint64_t size;
};


#endif // THEMIS_MAPRED_STREAM_INFO_H
