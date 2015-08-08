#ifndef MAPRED_READ_REQUEST_H
#define MAPRED_READ_REQUEST_H

#include <set>
#include <stdint.h>
#include <string>

#include "core/Resource.h"

/**
   A ReadRequest is a request to read part or all of a particular file.
 */
class ReadRequest : public Resource {
public:
  enum Protocol {
    FILE, /// A file on the local filesystem
    HDFS, /// A file on HDFS
    INVALID_PROTOCOL /// An invalid or unknown protocol
  };

  /// Constructor
  /**
     \param jobIDs the set of job IDs that should consume the data being read

     \param protocol the protocol that should be used to read the file

     \param host the machine hosting the file

     \param port the port on which the machine given by host is serving the file

     \param path path to the file, relative to the root of the hosting machine

     \param offset an offset, in bytes, where the read should begin

     \param length the length of the read

     \param diskID the ID of the disk to read from
   */
  ReadRequest(
    const std::set<uint64_t>& jobIDs, Protocol protocol,
    const std::string& host, uint32_t port,
    const std::string& path, uint64_t offset, uint64_t length, uint64_t diskID);

  /// Constructor
  /**
     \param diskID the ID of the disk to read from

     \param localPath a path on the local filesystem
   */
  ReadRequest(const std::string& localPath, uint64_t diskID);

  /// Construct a ReadRequest from a URL
  /**
     The URL scheme of the provided URL is as follows:

     <protocol>://<hostname>(:<port>)/path

     \param jobIDs the set of job IDs that should consume the data being read

     \param url the URL from which this ReadRequest should be constructed

     \param offset the offset, in bytes, where the read should begin

     \param length the size of the read

     \param diskID the disk associated with the read request

     \return a new ReadRequest corresponding to the URL
   */
  static ReadRequest* fromURL(
    const std::set<uint64_t>& jobIDs, const std::string& url, uint64_t offset,
    uint64_t length, uint64_t diskID);

  /// Get the text name of a protocol
  /**
     \param protocol the protocol to name

     \return the name of the protocol as a string
   */
  static std::string protocolName(Protocol protocol);

  /// \sa Resource::getCurrentSize
  uint64_t getCurrentSize() const;

  const Protocol protocol;
  const std::string host;
  const uint32_t port;
  const std::string path;
  const uint64_t offset;
  const uint64_t length;
  const uint64_t diskID;

  std::set<uint64_t> jobIDs;
};
#endif // MAPRED_READ_REQUEST_H
