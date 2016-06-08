#ifndef _TRITONSORT_UTILS_H
#define _TRITONSORT_UTILS_H

#include <boost/lexical_cast.hpp>
#include <list>
#include <json/json.h>"
#include <sched.h>
#include <string>
#include <vector>

#include "core/Socket.h"
#include "core/StatLogger.h"
#include "core/constants.h"

class Params;

struct OpenSocketArgs {
  const std::string& port;
  uint64_t numConnectingPeers;
  Params& params;
  const std::string& phaseName;
  const std::string& stageName;
  std::vector<std::vector<std::string> >& peerIPs;
  SocketArray& sockets;

  OpenSocketArgs(
    const std::string& _port, uint64_t _numConnectingPeers, Params& _params,
    const std::string& _phaseName, const std::string& _stageName,
    std::vector<std::vector<std::string> >& _peerIPs, SocketArray& _sockets)
    : port(_port),
      numConnectingPeers(_numConnectingPeers),
      params(_params),
      phaseName(_phaseName),
      stageName(_stageName),
      peerIPs(_peerIPs),
      sockets(_sockets) {
  }
};

typedef std::vector<std::vector<std::string> > IPList;

// Parse a newline-delimited list of disk paths
void parseDiskList(StringList& diskList,
                   const std::string& diskListFile);
bool fileExists(const char* fileName);
bool fileExists(const std::string& fileName);

void setThreadName(const std::string& name);

uint64_t getFileSize(const char* filename);
uint64_t getFileSize(int fd);

void strip(std::string& str);

template <typename T, typename Container> void parseCommaDelimitedList(
  Container& list, const std::string& str) {

  uint64_t currToken = 0;

  while (currToken != std::string::npos) {
    uint64_t tokenEnd = str.find_first_of(',', currToken);
    uint64_t tokenLength;

    if (tokenEnd != std::string::npos) {
      tokenLength = tokenEnd - currToken;
    } else {
      tokenLength = std::string::npos;
    }

    std::string token = str.substr(currToken, tokenLength);
    strip(token);

    if (token.size() > 0) {
      T convertedToken = boost::lexical_cast<T>(token);
      list.push_back(convertedToken);
    }

    if (tokenEnd != std::string::npos) {
      currToken = tokenEnd + 1;
    } else {
      currToken = std::string::npos;
    }
  }
}

/**
   Write data to a file or socket in a blocking fashion.

   \param fd the file or socket to write to

   \param buffer a buffer containing bytes to write

   \param size the number of bytes to write

   \param maxWriteSize the largest size of a single write() call

   \param alignment the alignment size for writes, set to 0 if not using direct
   IO

   \param description a string describing the file or socket to be used in
   printed error messages
 */
void blockingWrite(
  int fd, const uint8_t* buffer, uint64_t size, uint64_t maxWriteSize,
  uint64_t alignment, const char* description);

/**
   Read data from a file or socket in a blocking fashion.

   \param fd the file or socket to read from

   \param buffer a buffer where read bytes will be stored

   \param size the number of bytes to read

   \param maxReadSize the largest size of a single read() call

   \param alignment the alignment size for read, set to 0 if not using direct IO

   \param description a string describing the file or socket to be used in
   printed error messages

   \return the number of bytes read
 */
uint64_t blockingRead(
  int fd, uint8_t* buffer, uint64_t size, uint64_t maxReadSize,
  uint64_t alignment, const char* description);

void logCPUAffinityMask(const std::string& typeName, uint64_t id,
                        cpu_set_t& cpuAffinityMask, uint64_t numCores);

void getHostname(std::string& hostnameOutStr);

void getShortHostname(std::string& shortHostnameOutStr);

std::string* getIPAddress(const std::string& interface);

void dumpParams(Params& params);

void loadJsonFile(const std::string& filename, Json::Value& value);
void loadJsonString(const char* str, uint32_t strLength, Json::Value& value);

// Limit the maximum amount of memory that the process can have
void limitMemorySize(Params& params);

/**
   Construct a list of peer IP addresses from peer list parameters. These
   addresses are intended to serve as identifiers for the peers, so only the
   address of the first interface is returned for each peer.

   \param params the global params object

   \param[out] peerIPs an output parameter corresponding to a list of interfaces
   for each peer
 */
void parsePeerList(Params& params, IPList& peerIPs);

/**
   Open sockets that will receive data.

   \param port the port to listen on

   \param numConnectingPeers the number of peers connecting to this peer

   \param params the global params object

   \param phaseName the name of the phase

   \param stageName the name of the stage

   \param peerIPs the list of peers constructed by parsePeerList()

   \param sockets[out] the list of sockets that will receive data
 */
void openReceiverSockets(
  const std::string& port, uint64_t numConnectingPeers, Params& params,
  const std::string& phaseName, const std::string& stageName, IPList& peerIPs,
  SocketArray& sockets);

/**
   A threaded function that opens receiver sockets.

   \param args an OpenSocketArgs
 */
void* openReceiverSocketsThreaded(void* args);

/**
   Open sockets that will send data.

   \param port the port to connect to

   \param params the global params object

   \param phaseName the name of the phase

   \param stageName the name of the stage

   \param peerIPs the list of peers constructed by parsePeerList()

   \param sockets[out] the list of sockets that will send data
 */
void openSenderSockets(
  const std::string& port, Params& params, const std::string& phaseName,
  const std::string& stageName, IPList& peerIPs, SocketArray& sockets);

/**
   A threaded function that opens sender sockets.

   \param args an OpenSocketArgs
 */
void* openSenderSocketsThreaded(void* args);

/**
   Open all sockets that will send and receive data. In order to prevent
   deadlock, sender and receiver sockets are opened simultaneously in separate
   threads. This function will block until all sender and receiver sockets are
   opened.

   \param port the port to listen on

   \param numConnectingPeers the number of peers connecting to this peer

   \param params the global params object

   \param phaseName the name of the phase

   \param stageName the name of the stage

   \param senderPeerIPs the subset of the list of peers constructed by
   parsePeerList() that we should connect to

   \param receiverPeerIPs the list of peers constructed by parsePeerList(). Used
   to determine peerID from IP address for incoming connections.

   \param senderSockets[out] the list of sockets that will send data

   \param receiverSockets[out] the list of sockets that will receive data
 */
void openAllSockets(
  const std::string& port, uint64_t numConnectingPeers, Params& params,
  const std::string& phaseName, const std::string& stageName,
  IPList& senderPeerIPs, IPList& receiverPeerIPs,
  SocketArray& senderSockets, SocketArray& receiverSockets);


#endif //_TRITONSORT_UTILS_H
