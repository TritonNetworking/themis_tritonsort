#include <boost/lexical_cast.hpp>

#include "core/TritonSortAssert.h"
#include "mapreduce/common/URL.h"

namespace themis {

URL::URL(const std::string& url)
  : _fullURL(url) {
  // URL scheme is <scheme>://<hostname>(:<port>)/<path>

  size_t colonPositionInURL = url.find(':');

  ABORT_IF(colonPositionInURL == std::string::npos, "Invalid URL '%s'; could "
           "not parse scheme", url.c_str());

  _scheme.assign(url.substr(0, colonPositionInURL));

  size_t hostnameStart = colonPositionInURL + 3;

  size_t portStart = url.find(':', hostnameStart);
  size_t hostnameEnd = url.find('/', hostnameStart);

  ABORT_IF(hostnameEnd == std::string::npos, "Invalid URL '%s'; no path "
           "specified", url.c_str());

  if (portStart != std::string::npos && portStart + 1 < hostnameEnd) {
    // Port has been specified; parse it out
    try {
      _port = boost::lexical_cast<uint32_t>(
        url.substr(portStart + 1, hostnameEnd - (portStart + 1)));
    } catch (const boost::bad_lexical_cast& exception) {
      ABORT("Invalid URL '%s'; could not parse port", url.c_str());
    }
    _host.assign(url.substr(hostnameStart, portStart - hostnameStart));
  } else {
    _host.assign(url.substr(hostnameStart, hostnameEnd - hostnameStart));
  }

  _path.assign(url.substr(hostnameEnd));
}

const std::string& URL::scheme() const {
  return _scheme;
}

const std::string& URL::host() const {
  return _host;
}

uint32_t URL::port() const {
  return _port;
}

const std::string& URL::path() const {
  return _path;
}

const std::string& URL::fullURL() const {
  return _fullURL;
}

}  // namespace themis
