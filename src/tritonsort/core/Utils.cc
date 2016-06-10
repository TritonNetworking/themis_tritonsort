#include <algorithm>
#include <arpa/inet.h>
#include <cstdio>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <ifaddrs.h>
#include <netdb.h>
#include <sstream>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "core/File.h"
#include "core/Params.h"
#include "core/StatusPrinter.h"
#include "core/Thread.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/constants.h"

// TODO: Super inefficient; I doubt this will bottleneck us at all, but
// we may want to improve this implementation if it does.

void parseDiskList(StringList& diskList,
                   const std::string& diskListFile) {
  std::ifstream fileStream(diskListFile.c_str(), std::ifstream::in);

  char diskLine[FILENAME_MAX];

  while (fileStream.good()) {
    memset(diskLine, '\0', FILENAME_MAX);

    fileStream.getline(diskLine, FILENAME_MAX);

    std::string disk(diskLine);
    if (disk.size() > 0) {
      diskList.push_back(disk);
    }
  }

  fileStream.close();
}

bool fileExists(const char* fileName) {
  struct stat statbuf;
  int status = stat(fileName, &statbuf);

  return status == 0;
}

uint64_t getFileSize(int fd) {
  struct stat statbuf;

  int status = fstat(fd, &statbuf);

  if (status != 0) {
    ABORT("fstat() failed with error %d: %s", errno, strerror(errno));
  }

  return statbuf.st_size;
}

uint64_t getFileSize(const char* filename) {
  struct stat statbuf;

  int status = stat(filename, &statbuf);

  if (status == -1) {
    ABORT("stat(%s) failed with error %d: %s", filename, errno,
          strerror(errno));
  }

  return statbuf.st_size;
}

bool fileExists(const std::string& fileName) {
  return fileExists(fileName.c_str());
}

void setThreadName(const std::string& name) {
  char threadName[17];
  memset(threadName, 0, 17);

  memcpy(threadName, name.c_str(), std::min<uint64_t>(name.size(), 16));

  if (prctl(PR_SET_NAME, threadName) == -1) {
    ABORT("prctl() returned status %d: %s", errno, strerror(errno));
  }
}

void strip(std::string& str) {
  if (str.empty()) {
    return;
  }
  const char* whitespaceChars = " \t";
  size_t startIndex = str.find_first_not_of(whitespaceChars);
  size_t endIndex = str.find_last_not_of(whitespaceChars);

  if (startIndex == std::string::npos || endIndex == std::string::npos) {
    // String is all whitespace
    str = std::string();
    return;
  }

  std::string tmp(str);

  str = tmp.substr(startIndex, endIndex - startIndex + 1);
}

void blockingWrite(
  int fd, const uint8_t* buffer, uint64_t size, uint64_t maxWriteSize,
  uint64_t alignment, const char* description) {

  uint64_t bytesTransferred = 0;
  int bytesWritten;

  while (bytesTransferred < size) {
    uint64_t writeSize = size - bytesTransferred;
    if (maxWriteSize > 0) {
        writeSize = std::min<uint64_t>(writeSize, maxWriteSize);
    }

    ABORT_IF(alignment > 0 && writeSize % alignment != 0,
             "Write of size %llu not aligned to %llu. Should have disabled "
             "O_DIRECT before issuing this write", writeSize, alignment);

    bytesWritten = write(fd, buffer + bytesTransferred, writeSize);

    ABORT_IF(bytesWritten == 0,
             "write() of size %llu to %s (descriptor %d) returned 0 bytes",
             writeSize, description, fd);
    ABORT_IF(bytesWritten < 0,
             "write() of size %llu to %s (descriptor %d) failed with error %d: "
             "%s", writeSize, description, fd, errno, strerror(errno));

    bytesTransferred += bytesWritten;
  }
}

uint64_t blockingRead(
  int fd, uint8_t* buffer, uint64_t size, uint64_t maxReadSize,
  uint64_t alignment, const char* description) {

  uint64_t bytesTransferred = 0;
  int bytesRead;

  while (bytesTransferred < size) {
    uint64_t readSize = size - bytesTransferred;
    if (maxReadSize > 0) {
      readSize = std::min<uint64_t>(readSize, maxReadSize);
    }

    ABORT_IF(alignment > 0 && readSize % alignment != 0,
             "Read of size %llu not aligned to %llu. Should have disabled "
             "O_DIRECT before issuing this read", readSize, alignment);

    bytesRead = read(fd, buffer + bytesTransferred, readSize);

    ABORT_IF(bytesRead < 0,
             "read() of %llu bytes on on %s (descriptor %d) returned error "
             "code %d: %s", readSize, description, fd, errno, strerror(errno));

    if (bytesRead == 0) {
      // Hit EOF before reading size bytes.
      break;
    }

    bytesTransferred += bytesRead;
  }
  return bytesTransferred;
}

void logCPUAffinityMask(const std::string& typeName, uint64_t id,
                        cpu_set_t& cpuAffinityMask, uint64_t numCores) {
  std::ostringstream maskBitStringStream;

  for (uint64_t i = 0; i < numCores; i++) {
    maskBitStringStream << CPU_ISSET(i, &cpuAffinityMask);
  }

  StatusPrinter::add("%s\t%d\tcpu_mask\t%s", typeName.c_str(), id,
                     maskBitStringStream.str().c_str());
}


void getHostname(std::string& hostnameOutStr) {
  char hostname[HOST_NAME_MAX + 1];
  hostname[HOST_NAME_MAX] = 0;

  if (gethostname(hostname, HOST_NAME_MAX)) {
    ABORT("gethostname() failed with error %d: %s", errno, strerror(errno));
  }

  hostnameOutStr.assign(hostname);
}

void getShortHostname(std::string& shortHostnameOutStr) {
  std::string hostname;

  getHostname(hostname);
  shortHostnameOutStr.assign(hostname);

  size_t firstDotLocation = shortHostnameOutStr.find_first_of('.');

  if (firstDotLocation != std::string::npos) {
    shortHostnameOutStr.resize(firstDotLocation + 1);
  }
}


std::string* getIPAddress(const std::string& interface) {
  struct ifaddrs* ifaddrsList;

  if (getifaddrs(&ifaddrsList) == -1) {
    ABORT("getifaddrs() failed with error %d: %s", errno, strerror(errno));
  }

  struct sockaddr* addr = NULL;
  int family = 0;

  for (struct ifaddrs* iter = ifaddrsList; iter != NULL;
       iter = iter->ifa_next) {
    family = iter->ifa_addr->sa_family;

    if (interface.compare(iter->ifa_name) == 0) {
      // Found the interface we're looking for

      if (family == AF_INET) {
        addr = iter->ifa_addr;
        break;
      }
    }
  }

  if (addr == NULL) {
    ABORT("Can't find address for interface %s", interface.c_str());
  }

  char host[NI_MAXHOST];

  int status = getnameinfo(addr, sizeof(struct sockaddr_in), host,
                           NI_MAXHOST, NULL, 0, NI_NUMERICHOST);

  if (status != 0) {
    ABORT("getnameinfo() failed: %s", gai_strerror(status));
  }
  std::string* hostIPString = new std::string(host);

  freeifaddrs(ifaddrsList);

  return hostIPString;
}

void dumpParams(Params& params) {
  const std::string& logDirName = params.get<std::string>("LOG_DIR");

  std::string hostname;
  getHostname(hostname);

  std::ostringstream oss;
  oss << logDirName << '/' << hostname << "_params.log";
  params.dump(oss.str());
}


void generateLogPrefixString(std::string& outputStringRef,
                             const std::string& loggerName) {
  outputStringRef.assign(loggerName);
}

void generateLogPrefixString(std::string& outputStringRef,
                             const std::string& stageOrPoolName, uint64_t id) {

  std::ostringstream oss;
  oss << stageOrPoolName << '\t' << id;
  outputStringRef.assign(oss.str());
}

void generateLogPrefixString(std::string& outputStringRef,
                             const std::string& stageName, uint64_t id,
                             const std::string& poolName, uint64_t poolNumber) {
  std::ostringstream oss;
  oss << stageName << '\t' << id << '\t' << poolName << '\t' << poolNumber;
  outputStringRef.assign(oss.str());
}

void loadJsonFile(const std::string& filename, Json::Value& jsonValue) {
  File jsonFile(filename);
  jsonFile.open(File::READ);

  uint64_t fileSize = jsonFile.getCurrentSize();
  uint8_t* fileContentsBuffer = new uint8_t[fileSize];

  jsonFile.read(fileContentsBuffer, fileSize);

  loadJsonString(
    reinterpret_cast<char*>(fileContentsBuffer), fileSize, jsonValue);

  delete[] fileContentsBuffer;
  jsonFile.close();
}

void loadJsonString(
  const char* str, uint32_t strLength, Json::Value& jsonValue) {

  Json::Reader reader;

  bool parseSuccessful = reader.parse(str, str + strLength, jsonValue);

  ABORT_IF(!parseSuccessful, "Failed to parse: %s",
           reader.getFormattedErrorMessages().c_str());
}

void limitMemorySize(Params& params) {
  uint64_t memMax = params.get<uint64_t>("MEM_SIZE");

  struct rlimit64 memLimits;
  memLimits.rlim_cur = memMax;
  memLimits.rlim_max = memMax;

  ABORT_IF(setrlimit64(RLIMIT_AS, &memLimits) == -1, "setrlimit64() "
           "failed with error %d: %s", errno, strerror(errno));
}

void parsePeerList(Params& params, IPList& peerIPs) {
  // Grab the peer list string from params.
  std::string peerList = params.get<std::string>("PEER_LIST");

  // Get a list of interface IP addresses from the peer list
  std::vector<std::string> interfaces;
  parseCommaDelimitedList< std::string, std::vector<std::string> >(
    interfaces, peerList);

  // We need to output a list of lists of interfaces for each peer.
  uint64_t numInterfacesPerPeer = params.get<uint64_t>("NUM_INTERFACES");
  uint64_t numIPs = interfaces.size();
  ABORT_IF(numIPs % numInterfacesPerPeer != 0,
           "Number of interfaces per peer %llu must evenly divide the number "
           "of IPs in the peer list %llu", numInterfacesPerPeer, numIPs);
  uint64_t numPeers = numIPs / numInterfacesPerPeer;
  peerIPs.resize(numPeers);

  uint64_t peerID = 0;
  for (std::vector<std::string>::iterator iter = interfaces.begin();
       iter != interfaces.end(); iter++) {
    // Add the current interface to the current peer.
    peerIPs[peerID].push_back(*iter);

    if (peerIPs[peerID].size() == numInterfacesPerPeer) {
      // We've seen all interfaces for this peer, so move on to the next one.
      peerID++;
    }
  }

  // Set number of peers.
  params.add<uint64_t>("NUM_PEERS", peerIPs.size());

  // Set our IP address as the first interface.
  peerID = params.get<uint64_t>("MYPEERID");
  params.add<std::string>("MYIPADDRESS", peerIPs.at(peerID).at(0));
}

void openReceiverSockets(
  const std::string& port, uint64_t numConnectingPeers, Params& params,
  const std::string& phaseName, const std::string& stageName,
  IPList& peerIPs, SocketArray& sockets) {

  uint64_t socketsPerPeer = params.get<uint64_t>(
    "SOCKETS_PER_PEER." + phaseName + "." + stageName);
  uint64_t numConnections = numConnectingPeers * socketsPerPeer;
  uint64_t socketBufferSize = params.get<uint64_t>("TCP_RECEIVE_BUFFER_SIZE");
  uint64_t backlogSize = params.get<uint64_t>("ACCEPT_BACKLOG_SIZE");

  Socket listeningSocket;
  listeningSocket.listen(port, backlogSize);

  // Record the number of connections from each peer so we can determine flow
  // IDs.
  std::map<uint64_t, uint64_t> flowIDMap;

  StatusPrinter::add("Waiting to accept %lld connections", numConnections);

  for (uint64_t i = 0; i < numConnections; i++) {
    // Wait for a new connection.
    Socket* socket = listeningSocket.accept(0, socketBufferSize);
    const std::string& connectingAddress = socket->getAddress();

    // Find its peer ID in the list of peers.
    uint64_t peerID = 0;
    IPList::iterator iter;
    for (iter = peerIPs.begin(); iter != peerIPs.end(); iter++) {
      // Check all interfaces for this peer.
      bool foundIP = false;
      for (std::vector<std::string>::iterator interfaceIter = iter->begin();
           interfaceIter != iter->end(); interfaceIter++) {
        if (interfaceIter->compare(connectingAddress) == 0) {
          foundIP = true;
        }
      }

      if (foundIP) {
        break;
      }
      peerID++;
    }

    ABORT_IF(iter == peerIPs.end(),
             "Could not find connecting address %s in list of peers.",
             connectingAddress.c_str());

    socket->setPeerID(peerID);

    uint64_t& flowID = flowIDMap[peerID];
    socket->setFlowID(flowID);
    flowID++;

    // Store this socket so we can access it in the receiver.
    sockets.push_back(socket);
  }

  // Randomly shuffle socket array to distribute connections among workers.
  std::random_shuffle(sockets.begin(), sockets.end());

  StatusPrinter::add("Accepted all %llu connections", numConnections);

  listeningSocket.close();
}

void* openReceiverSocketsThreaded(void* args) {
  // Treat args as OpenSocketArgs and call the appropriate function.
  OpenSocketArgs* openSocketArgs = static_cast<OpenSocketArgs*>(args);
  openReceiverSockets(
    openSocketArgs->port, openSocketArgs->numConnectingPeers,
    openSocketArgs->params, openSocketArgs->phaseName,
    openSocketArgs->stageName, openSocketArgs->peerIPs,
    openSocketArgs->sockets);

  return NULL;
}

void openSenderSockets(
  const std::string& port, Params& params, const std::string& phaseName,
  const std::string& stageName, IPList& peerIPs, SocketArray& sockets) {

  uint64_t socketsPerPeer = params.get<uint64_t>(
    "SOCKETS_PER_PEER." + phaseName + "." + stageName);
  uint64_t socketBufferSize = params.get<uint64_t>("TCP_SEND_BUFFER_SIZE");
  uint64_t retryDelayInMicros = params.get<uint64_t>("CONNECT_RETRY_DELAY");
  uint64_t maxRetries = params.get<uint64_t>("CONNECT_MAX_RETRIES");

  StatusPrinter::add("Setting up %llu connections",
                     peerIPs.size() * socketsPerPeer);

  uint64_t peerID = 0;
  for (IPList::iterator iter = peerIPs.begin(); iter != peerIPs.end();
       iter++, peerID++) {
    // Open some number of sockets to this peer, round-robin across each of its
    // interfaces.
    std::vector<std::string>::iterator interfaceIter = iter->begin();
    for (uint64_t i = 0; i < socketsPerPeer; i++) {
      // Open a connection on the current interface.
      Socket* socket = new Socket();
      socket->connect(
        *interfaceIter, port, socketBufferSize, retryDelayInMicros, maxRetries);
      // Set the peer ID, although if we're only connecting to a subset of the
      // peers, this peer ID may not reflect the correct position in the global
      // peer list.
      socket->setPeerID(peerID);
      socket->setFlowID(i);

      // Store this socket so we can access it in the sender.
      sockets.push_back(socket);

      // Round-robin to the next interface.
      interfaceIter++;
      if (interfaceIter == iter->end()) {
        // We still have sockets but we ran out of interfaces, so repeat from
        // the first interface.
        interfaceIter = iter->begin();
      }
    }
  }

  // Randomly shuffle socket array to distribute connections among workers.
  std::random_shuffle(sockets.begin(), sockets.end());

  StatusPrinter::add("Established %llu connections", sockets.size());
}

void* openSenderSocketsThreaded(void* args) {
  // Treat args as OpenSocketArgs and call the appropriate function.
  OpenSocketArgs* openSocketArgs = static_cast<OpenSocketArgs*>(args);
  openSenderSockets(
    openSocketArgs->port, openSocketArgs->params, openSocketArgs->phaseName,
    openSocketArgs->stageName, openSocketArgs->peerIPs,
    openSocketArgs->sockets);

  return NULL;
}

void openAllSockets(
  const std::string& port, uint64_t numConnectingPeers, Params& params,
  const std::string& phaseName, const std::string& stageName,
  IPList& senderPeerIPs, IPList& receiverPeerIPs,
  SocketArray& senderSockets, SocketArray& receiverSockets) {

  // Wrap arguments in a struct so we can pass a single void* argument.
  OpenSocketArgs receiverArgs(
    port, numConnectingPeers, params, phaseName, stageName, receiverPeerIPs,
    receiverSockets);
  OpenSocketArgs senderArgs(
    port, numConnectingPeers, params, phaseName, stageName, senderPeerIPs,
    senderSockets);

  // Create thread objects that wrap the functions we want to call.
  themis::Thread openReceiverSocketsThread(
    "OpenReceiverSockets", &openReceiverSocketsThreaded);
  themis::Thread openSenderSocketsThread(
    "OpenSenderSockets", &openSenderSocketsThreaded);

  // Start both threads.
  openReceiverSocketsThread.startThread(&receiverArgs);
  openSenderSocketsThread.startThread(&senderArgs);

  // Wait for both threads to complete.
  openReceiverSocketsThread.stopThread();
  openSenderSocketsThread.stopThread();
}
