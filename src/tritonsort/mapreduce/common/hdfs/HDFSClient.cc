#include <json/reader.h>
#include <json/value.h>
#include <sstream>
#include <string.h>

#include "common/buffers/BaseBuffer.h"
#include "mapreduce/common/hdfs/HDFSClient.h"
#include "mapreduce/common/hdfs/HDFSReader.h"

HDFSClient::Response::Response() {
  capacity = INITIAL_MEMORY_SIZE;
  size = 0;

  memory = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * capacity));

  ABORT_IF(memory == NULL, "Allocating %lluB of memory for HDFSClient "
           "response failed", capacity);

  memset(memory, 0, capacity);
}

HDFSClient::Response::~Response() {
  if (memory != NULL) {
    free(memory);
    memory = NULL;
  }
}

void HDFSClient::Response::append(
  void* appendMemory, uint64_t appendSize) {

  uint64_t minCapacity = size + appendSize;

  if (minCapacity > capacity) {
    // Double the capacity of the array until it can fit all data
    while (capacity < minCapacity) {
      capacity *= 2;
    }

    uint8_t* newMemory = static_cast<uint8_t*>(
      realloc(memory, sizeof(uint8_t) * capacity));

    if (newMemory == NULL) {
      free(memory);
      memory = NULL;

      ABORT("Expanding memory for HDFSClient response to  %lluB failed",
            capacity);
    } else {
      memory = newMemory;
    }
  }

  memcpy(memory + size, appendMemory, appendSize);
  size += appendSize;
}

void HDFSClient::Response::asJSON(Json::Value& value) {
  Json::Reader reader;

  memory[size] = 0;

  bool parseSuccessful = reader.parse(
    reinterpret_cast<char*>(memory), reinterpret_cast<char*>(memory + size),
    value, false);

  ABORT_IF(
    !parseSuccessful, "Parsing HDFS response failed:\n\nResponse "
    "(%llu B):\n\n%sErrors:\n\n%s", size, memory,
    reader.getFormattedErrorMessages().c_str());
}

HDFSClient::SendStruct::SendStruct(const BaseBuffer& buffer)
  : data(buffer.getRawBuffer()),
    offset(0),
    length(buffer.getCurrentSize()) {
}


size_t HDFSClient::bufferReadCallback(
  void* resultBytes, size_t memberSize, size_t numMembers,
  void* callerData) {

  BaseBuffer* buffer = static_cast<BaseBuffer*>(callerData);

  uint64_t resultSize = memberSize * numMembers;

  // If the response is too big to fit in a buffer, we're probably getting some
  // form of error response from the server, which we'll catch with a code
  // check later on. So just ignore what you're getting and wait for the error
  // code.
  if (resultSize + buffer->getCurrentSize() <= buffer->getCapacity()) {
    buffer->append(static_cast<uint8_t*>(resultBytes), resultSize);
  }

  return resultSize;
}

size_t HDFSClient::responseReadCallback(
  void* resultBytes, size_t memberSize, size_t numMembers,
  void* callerData) {

  uint64_t resultSize = memberSize * numMembers;

  Response* response = static_cast<Response*>(callerData);
  response->append(resultBytes, resultSize);

  return resultSize;
}

size_t HDFSClient::hdfsReaderReadCallback(
  void* resultBytes, size_t memberSize, size_t numMembers, void* callerData) {

  HDFSReader* reader = static_cast<HDFSReader*>(callerData);

  return reader->handleHDFSRead(resultBytes, memberSize * numMembers);
}


size_t HDFSClient::bufferWriteCallback(
  void* writeMemory, size_t memberSize, size_t numMembers, void* callerData) {

  SendStruct* sendStruct = static_cast<SendStruct*>(callerData);

  uint64_t sendSize = std::min<uint64_t>(
    memberSize * numMembers, sendStruct->length - sendStruct->offset);

  if (sendSize > 0) {
    memcpy(writeMemory, sendStruct->data + sendStruct->offset, sendSize);
  }

  sendStruct->offset += sendSize;

  return sendSize;
}

size_t HDFSClient::nopWriteCallback(
  void* writeMemory, size_t memberSize, size_t numMembers, void* callerData) {
  // Tell libcurl we've processed all data, but don't actually do anything
  return memberSize * numMembers;
}

void HDFSClient::resetCURL() {
  // Reset the CURL handle's state
  curl_easy_reset(curl);

  // libcurl has problems with signals in multi-threaded environments, so turn
  // them off as recommended by
  // http://curl.haxx.se/libcurl/c/libcurl-tutorial.html#Multi-threading
  CURLcode status = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_NOSIGNAL) failed: %s",
           curl_easy_strerror(status));

  // Drop all header bytes on the floor
  status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, nopWriteCallback);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_HEADERFUNCTION) "
           "failed: %s", curl_easy_strerror(status));

  // Don't display a progress meter
  status = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_NOPROGRESS) "
           "failed: %s", curl_easy_strerror(status));

  // Don't use a proxy, even if there is an environment variable set for one
  status = curl_easy_setopt(curl, CURLOPT_PROXY, "");

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_PROXY) failed: %s",
           curl_easy_strerror(status));
}

void HDFSClient::performCURLRequest(const char* url) {

  CURLcode status = curl_easy_setopt(curl, CURLOPT_URL, url);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_URL) failed: %s",
           curl_easy_strerror(status));

  // Send the GET request
  status = curl_easy_perform(curl);

  ABORT_IF(status != CURLE_OK, "GET for URL '%s' failed: %s",
           url, curl_easy_strerror(status));
}

HDFSClient::HDFSClient(const std::string& host, uint32_t port) {
  std::ostringstream oss;
  oss << "http://" << host << ':' << port << "/webhdfs/v1";
  urlBase.assign(oss.str());

  curl = curl_easy_init();

  ABORT_IF(curl == NULL, "curl_easy_init() failed");

  resetCURL();
}

HDFSClient::~HDFSClient() {
  if (curl != NULL) {
    curl_easy_cleanup(curl);
    curl = NULL;
  }
}

void HDFSClient::init() {
  // I hate you, libcurl!
  ABORT_IF(curl_global_init(CURL_GLOBAL_ALL) != 0,
           "libcurl initialization failed");
}

void HDFSClient::cleanup() {
  // Die in a fire, libcurl!
  curl_global_cleanup();
}

void HDFSClient::read(
  const std::string& path, uint64_t offset, uint64_t length,
  BaseBuffer& buffer) {

  resetCURL();

  // Write data into the buffer as it is received
  setCurlGetCallbackFunction(bufferReadCallback, &buffer);

  // Make sure we follow the redirect to the appropriate data replica
  CURLcode status = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_FOLLOWLOCATION) "
           "failed: %s", curl_easy_strerror(status));

  uint64_t oldBufferSize = buffer.getCurrentSize();

  // Construct REST request from the base URL
  std::ostringstream oss;

  oss << urlBase << path << "?op=OPEN&offset=" << offset
      << "&length=" << length;

  std::string urlStr(oss.str());
  const char* url = urlStr.c_str();

  performCURLRequest(url);
  assertResponseCode(url, 200);

  // At this point, buffer should contain the data retrieved
  uint64_t bytesReceived = buffer.getCurrentSize() - oldBufferSize;
  ABORT_IF(bytesReceived != length, "Buffer should have received %llu bytes "
           "from '%s', but received %llu", length, url, bytesReceived);
}

void HDFSClient::read(
  const std::string& path, uint64_t offset, uint64_t length,
  HDFSReader& reader) {

  resetCURL();

  setCurlGetCallbackFunction(hdfsReaderReadCallback, &reader);

  // Make sure we follow data replica redirection
  CURLcode status = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_FOLLOWLOCATION) "
           "failed: %s", curl_easy_strerror(status));

  std::ostringstream oss;

  oss << urlBase << path << "?op=OPEN&offset=" << offset
      << "&length=" << length;

  std::string urlStr(oss.str());

  const char* url = urlStr.c_str();

  performCURLRequest(url);
  assertResponseCode(url, 200);
}

uint64_t HDFSClient::length(const std::string& path) {
  Json::Value statStruct;

  resetCURL();

  stat(path, statStruct);
  abortIfResponseIsException(statStruct, "stat", path);

  return statStruct["FileStatus"]["length"].asUInt64();
}

void HDFSClient::write(
  const std::string& path, const char* operation, const char* method,
  const BaseBuffer& buffer, uint32_t expectedWriteResponseCode) {
  // Due to sub-par implementation of "Expect: 100-Continue" in web servers,
  // this is a two-step process; we can't just follow the redirect
  // automatically.

  resetCURL();

  setCurlMethod(method);

  // First, issue a create PUT and snarf out the redirect URL.
  std::ostringstream oss;
  oss << urlBase << path << operation;

  std::string urlStr(oss.str());
  const char* url = urlStr.c_str();

  // Expect a 307 redirect
  performCURLRequest(url);
  assertResponseCode(url, 307);

  // Second, issue a create PUT to the provided redirect URL with the buffer's
  // contents

  char* redirectURL = NULL;

  CURLcode status = curl_easy_getinfo(
    curl, CURLINFO_REDIRECT_URL, &redirectURL);

  ABORT_IF(status != CURLE_OK, "curl_easy_getinfo(CURLOPT_REDIRECT_URL) "
           "failed: %s", curl_easy_strerror(status));

  std::string redirectURLStr(redirectURL);

  // Tell CURL to prepare to upload
  status = curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_UPLOAD) "
           "failed: %s", curl_easy_strerror(status));

  SendStruct sendStruct(buffer);

  // Transmit all data in the provided buffer
  setCurlPutCallbackFunction(bufferWriteCallback, &sendStruct);

  performCURLRequest(redirectURLStr.c_str());
  assertResponseCode(redirectURLStr.c_str(), expectedWriteResponseCode);
}

void HDFSClient::create(
  const std::string& path, const BaseBuffer& buffer, uint32_t replication) {
  std::ostringstream oss;
  oss << "?op=CREATE&replication=" << replication;

  std::string urlStr(oss.str());

  write(path, urlStr.c_str(), "PUT", buffer, 201);
}

void HDFSClient::append(const std::string& path, const BaseBuffer& buffer) {
  write(path, "?op=APPEND", "POST", buffer, 200);
}

void HDFSClient::rm(const std::string& path, bool recursive) {
  Json::Value response;

  std::ostringstream oss;
  oss << urlBase << path << "?op=DELETE&recursive=";

  if (recursive) {
    oss << "true";
  } else {
    oss << "false";
  }

  std::string urlStr(oss.str());

  const char* url = urlStr.c_str();

  getResponseFromHTTPRequest(url, response, "DELETE");
  assertResponseCode(url, 200);

  abortIfResponseIsException(response, "rm", path);
}

bool HDFSClient::fileExists(const std::string& path) {
  Json::Value statStruct;

  stat(path, statStruct);

  if (statStruct.isMember("RemoteException")) {
    // If the server returned a FileNotFoundException, the file doesn't exist
    if (statStruct["RemoteException"]["exception"] == "FileNotFoundException") {
      return false;
    } else {
      // If it returned some other exception, we should abort
      abortIfResponseIsException(statStruct, "stat", path);
      return false;
    }
  } else {
    if (statStruct["FileStatus"]["type"].asString() == "FILE") {
      // The path exists and is a file
      return true;
    } else {
      // The path exists, but it's a directory
      return false;
    }
  }
}

void HDFSClient::stat(
  const std::string& path, Json::Value& statStruct) {
  // Construct REST request from the base URL
  std::ostringstream oss;

  oss << urlBase << path << "?op=GETFILESTATUS";

  std::string urlStr(oss.str());
  const char* url = urlStr.c_str();

  getResponseFromHTTPRequest(url, statStruct);
}

void HDFSClient::getResponseFromHTTPRequest(
  const char* url, Json::Value& jsonStruct, const char* directive) {
  // Make sure we're using the right directive (by default, this is just a GET
  // request)
  setCurlMethod(directive);

  // Parse the response as a JSON object
  Response response;

  setCurlGetCallbackFunction(responseReadCallback, &response);

  performCURLRequest(url);

  response.asJSON(jsonStruct);
}

void HDFSClient::abortIfResponseIsException(
  Json::Value& response, const char* operation, const std::string& path) {

  if (response.isMember("RemoteException")) {
    if (response["RemoteException"].isMember("javaClassName")) {
      ABORT("Remote exception when trying to %s '%s': %s in %s: %s",
            operation, path.c_str(),
            response["RemoteException"]["exception"].asCString(),
            response["RemoteException"]["javaClassName"].asCString(),
            response["RemoteException"]["message"].asCString());
    } else {
      ABORT("Remote exception when trying to %s '%s': %s: %s",
            operation, path.c_str(),
            response["RemoteException"]["exception"].asCString(),
            response["RemoteException"]["message"].asCString());
    }
  }
}

void HDFSClient::setCurlMethod(const char* method) {
  CURLcode status = curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_CUSTOMREQUEST) "
           "failed: %s", curl_easy_strerror(status));
}

void HDFSClient::setCurlPutCallbackFunction(
  CallbackFunction function, void* callerData) {

  CURLcode status = curl_easy_setopt(
    curl, CURLOPT_READFUNCTION, function);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_WRITEFUNCTION) "
           "failed: %s", curl_easy_strerror(status));

  status = curl_easy_setopt(curl, CURLOPT_READDATA, callerData);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_WRITEDATA) failed: %s",
           curl_easy_strerror(status));
}

void HDFSClient::setCurlGetCallbackFunction(
  CallbackFunction function, void* callerData) {

  CURLcode status = curl_easy_setopt(
    curl, CURLOPT_WRITEFUNCTION, function);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_READFUNCTION) "
           "failed: %s", curl_easy_strerror(status));

  status = curl_easy_setopt(curl, CURLOPT_WRITEDATA, callerData);

  ABORT_IF(status != CURLE_OK, "curl_easy_setopt(CURLOPT_READDATA) failed: %s",
           curl_easy_strerror(status));
}

void HDFSClient::assertResponseCode(
  const char* url, long requiredResponseCode) {

  // Should have received a 200 OK from the server
  long responseCode = 0;

  CURLcode status = curl_easy_getinfo(
    curl, CURLINFO_RESPONSE_CODE, &responseCode);

  ABORT_IF(status != CURLE_OK, "curl_easy_getinfo(CURLINFO_RESPONSE_CODE) "
           "failed: %s", curl_easy_strerror(status));

  ABORT_IF(responseCode != requiredResponseCode, "Read from URL '%s' returned "
           "response code %ld (expected %ld)", url, responseCode,
           requiredResponseCode);

}
