#include <boost/lexical_cast.hpp>

#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/common/URL.h"

ReadRequest::ReadRequest(
  const std::set<uint64_t>& _jobIDs, Protocol _protocol,
  const std::string& _host, uint32_t _port,
  const std::string& _path, uint64_t _offset, uint64_t _length,
  uint64_t _diskID)
  : protocol(_protocol),
    host(_host),
    port(_port),
    path(_path),
    offset(_offset),
    length(_length),
    diskID(_diskID),
    jobIDs(_jobIDs) {

}

ReadRequest::ReadRequest(const std::string& localPath, uint64_t _diskID)
  : protocol(FILE),
    host("INVALID"),
    port(std::numeric_limits<uint32_t>::max()),
    path(localPath),
    offset(0),
    length(getFileSize(localPath.c_str())),
    diskID(_diskID) {
}

ReadRequest* ReadRequest::fromURL(
  const std::set<uint64_t>& jobIDs, const std::string& urlStr, uint64_t offset,
  uint64_t length, uint64_t diskID) {
  // URL scheme is <protocol>://<hostname>(:<port>)/<path>

  URL url(urlStr);

  const std::string& scheme = url.scheme();

  Protocol protocol = INVALID_PROTOCOL;

  if (scheme == "local") {
    protocol = FILE;
  } else if (scheme == "hdfs") {
    protocol = HDFS;
  } else {
    ABORT("Invalid URL '%s'; unknown protocol '%s'", urlStr.c_str(),
          scheme.c_str());
  }

  return new ReadRequest(
    jobIDs, protocol, url.host(), url.port(), url.path(), offset, length,
    diskID);
}

std::string ReadRequest::protocolName(Protocol protocol) {
  std::string protocolName;

  switch (protocol) {
  case FILE:
    protocolName.assign("local");
    break;
  case HDFS:
    protocolName.assign("hdfs");
    break;
  case INVALID_PROTOCOL:
    protocolName.assign("invalid");
    break;
  }

  return protocolName;
}

uint64_t ReadRequest::getCurrentSize() const {
  // Return the actual length of the read request so that reader bytes consumed
  // will be calculated correctly.
  return length;
}
