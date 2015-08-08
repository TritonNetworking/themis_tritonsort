#ifndef THEMIS_COMMON_URL_H
#define THEMIS_COMMON_URL_H

#include <stdint.h>
#include <string>

/**
   This class parses a URL string into its component parts, making it easy to
   access pieces of a URL
 */
class URL {
public:
  /// Constructor
  /**
     \param urlStr a string of the form "<protocol>://<host>(:port)/<path>"
     that will be parsed
   */
  URL(const std::string& urlStr);

  /// Destructor
  virtual ~URL() {}

  /// \return the URL's scheme (http, ftp, hdfs, etc.)
  const std::string& scheme() const;

  /// \return the URL's hostname
  const std::string& host() const;

  /// \return the URL's port number, or 0 if no port number has been specified
  uint32_t port() const;

  /// \return the URL's absolute path
  const std::string& path() const;

  /// \return the full URL
  const std::string& fullURL() const;

private:
  std::string _scheme;
  std::string _host;
  uint32_t _port;
  std::string _path;

  std::string _fullURL;
};


#endif // THEMIS_COMMON_URL_H
