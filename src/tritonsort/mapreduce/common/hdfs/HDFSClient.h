#ifndef THEMIS_MAPREDUCE_HDFS_CLIENT_H
#define THEMIS_MAPREDUCE_HDFS_CLIENT_H

#include <curl/curl.h>
#include <map>
#include <pthread.h>
#include <stdint.h>
#include <string>

class BaseBuffer;
class HDFSReader;

namespace Json {
  class Value;
};

/**
   HDFSClient provides a convenient wrapper around the WebHDFS interface to
   HDFS.
 */
class HDFSClient {
public:
  /// Constructor
  /**
     \param host the hostname of the HDFS namenode
     \param port the WebHDFS port for the namenode
   */
  HDFSClient(const std::string& host, uint32_t port);

  /// Destructor
  virtual ~HDFSClient();

  /// Perform static initialization required by CURL
  static void init();

  /// Perform static teardown required by CURL
  static void cleanup();

  /// Read from a file
  /**
     \param path path to the file relative to the root of the HDFS filesystem

     \param offset offset at which the read should start

     \param length length of the read

     \param buffer buffer into which to read data (assumed to have at least
     length bytes of free space)
   */
  void read(
    const std::string& path, uint64_t offset, uint64_t length,
    BaseBuffer& buffer);

  /// Read from a file
  /**
     As bytes are received, they will be passed to the provided HDFSReader's
     handleHDFSRead method for handling.

     \param path path to the file relative to the root of the HDFS filesystem

     \param offset offset at which the read should start

     \param length length of the read

     \param reader the HDFSReader that will handle the data being read
   */
  void read(const std::string& path, uint64_t offset, uint64_t length,
            HDFSReader& reader);

  /// Get the length of a file
  /**
     \param path the file whose length is requested

     \return the length of the file in bytes
   */
  uint64_t length(const std::string& path);

  /// Create a file
  /**
     \param path path to the file relative to the root of the HDFS filesystem

     \param buffer a buffer whose contents will be written as the first bytes
     of the new file

     \param replication the replication factor of the new file
   */
  void create(
    const std::string& path, const BaseBuffer& buffer,
    uint32_t replication = 1);

  /// Append to an existing file
  /**
     \param path path to the file relative to the root of the HDFS filesystem

     \param buffer a buffer whose contents will be appended to the file
   */
  void append(const std::string& path, const BaseBuffer& buffer);

  /// Delete a path on HDFS
  /**
     \param path path relative to the root of the HDFS filesystem

     \param recursive if path is a directory and this is true, entire subtree
     rooted at this path will be deleted
   */
  void rm(const std::string& path, bool recursive = false);

  /// Check for existence of a path
  /**
     \param path the path whose existence is to be checked

     \return true if the file exists in HDFS, false if not
   */
  bool fileExists(const std::string& path);

private:
  class Response {
  public:
    Response();
    virtual ~Response();

    void append(void* appendMemory, uint64_t appendSize);
    void asJSON(Json::Value& value);
  private:
    // Response memory will be dynamically grown on demand, but we'll start
    // with 16KiB for responses
    static const uint64_t INITIAL_MEMORY_SIZE = 16384;

    uint8_t* memory;
    uint64_t size;
    uint64_t capacity;
  };

  class SendStruct {
  public:
    const uint8_t* data;
    uint64_t offset;
    uint64_t length;

    SendStruct(const BaseBuffer& buffer);
  };

  typedef size_t (*CallbackFunction)(void*, size_t, size_t, void*);

  static size_t bufferReadCallback(
    void* resultBytes, size_t memberSize, size_t numMembers, void* callerData);

  static size_t responseReadCallback(
    void* resultBytes, size_t memberSize, size_t numMembers, void* callerData);

  static size_t hdfsReaderReadCallback(
    void* resultBytes, size_t memberSize, size_t numMembers, void* callerData);

  static size_t bufferWriteCallback(
    void* writeMemory, size_t memberSize, size_t numMembers, void* callerData);

  static size_t nopWriteCallback(
    void* writeMemory, size_t memberSize, size_t numMembers, void* callerData);

  void write(
    const std::string& path, const char* operation, const char* method,
    const BaseBuffer& buffer, uint32_t expectedWriteResponseCode);

  void stat(const std::string& path, Json::Value& statStruct);

  void getResponseFromHTTPRequest(
    const char* url, Json::Value& jsonStruct, const char* directive = "GET");

  void abortIfResponseIsException(
    Json::Value& response, const char* operation, const std::string& path);

  void setCurlMethod(const char* method);
  void setCurlPutCallbackFunction(
    CallbackFunction function, void* callerData);
  void setCurlGetCallbackFunction(
    CallbackFunction function, void* callerData);

  void resetCURL();

  void performCURLRequest(const char* url);
  void assertResponseCode(const char* url, long requiredResponseCode);

  std::string urlBase;
  CURL* curl;
};

#endif // THEMIS_MAPREDUCE_HDFS_CLIENT_H
