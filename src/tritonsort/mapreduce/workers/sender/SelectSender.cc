#include <sys/select.h>

#include "mapreduce/workers/sender/SelectSender.h"

SelectSender::SelectSender(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  SocketArray& sockets, uint64_t numSenders, uint64_t maxSendSize,
  uint64_t selectTimeout, bool enhancedNetworkLogging, const Params& params)
  : BaseSender(
    phaseName, stageName, id, sockets, numSenders, maxSendSize,
    enhancedNetworkLogging, params),
    selectTimeoutMicros(selectTimeout),
    numSocketsWithData(0),
    maxFD(-1) {
}

void SelectSender::run() {
  fd_set writeDescriptors;

  FD_ZERO(&socketsWithData);
  FD_ZERO(&writeDescriptors);


  while (numCompletedPeers < connections.size()) {
    if (numSocketsWithData + numCompletedPeers < connections.size()) {
      // We have at least once open socket without data, so get more.
      getMoreWork();
    }

    if (numSocketsWithData > 0) {
      // Select on the sockets with data so we can send to whatever peers
      // can receive data right now.
      writeDescriptors = socketsWithData;
      int status = -1;

      if (numSocketsWithData + numCompletedPeers < connections.size()) {
        // We're missing data for at least one socket, so don't select()
        // forever. Instead use a timeout so we can get data for other sockets
        // if we can't send to any of the ones we already have data for.

        struct timeval selectTimeout;
        selectTimeout.tv_sec = selectTimeoutMicros / 1000000;
        selectTimeout.tv_usec = selectTimeoutMicros % 1000000;

        status = select(
          maxFD + 1, NULL, &writeDescriptors, NULL, &selectTimeout);
      } else {
        // We have data for all sockets, so issue a select with no timeout.
        status = select(maxFD + 1, NULL, &writeDescriptors, NULL, NULL);
      }

      ABORT_IF(status == -1, "select() failed with error %d: %s",
               errno, strerror(errno));

      if (status > 0) {
        // Some sockets can send data.
        for (ConnectionList::iterator iter = connections.begin();
             iter != connections.end(); iter++) {
          if (!(*iter)->socket.closed() &&
              FD_ISSET((*iter)->socket.getFD(), &writeDescriptors)) {
            send(*iter);
          }
        }
      }
    }
  }
}

void SelectSender::handleNewBuffer(Connection* connection) {
  // Add this connection to the fd set.
  ASSERT(connection->socket.getFD() != -1, "Tried to add -1 fd to set.");
  FD_SET(connection->socket.getFD(), &socketsWithData);
  numSocketsWithData++;

  maxFD = std::max(connection->socket.getFD(), maxFD);
}

void SelectSender::handleEmptyBuffer(Connection* connection) {
  // Remove this connection from the fd set.
  ASSERT(connection->socket.getFD() != -1, "Tried to remove -1 fd from set.");
  FD_CLR(connection->socket.getFD(), &socketsWithData);
  numSocketsWithData--;

  if (connection->socket.getFD() == maxFD) {
    // This was the max file descriptor, so compute a new one.
    maxFD = -1;
    for (ConnectionList::iterator iter = connections.begin();
         iter != connections.end(); iter++) {
      if (!(*iter)->socket.closed()
          && FD_ISSET((*iter)->socket.getFD(), &socketsWithData)) {
        maxFD = std::max((*iter)->socket.getFD(), maxFD);
      }
    }
  }
}

BaseWorker* SelectSender::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t maxSendSize = params.get<uint64_t>(
    "SEND_SOCKET_SYSCALL_SIZE");

  SocketArray* sockets = dependencies.get<SocketArray>(stageName, "sockets");

  uint64_t numSenders = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t selectTimeout = params.get<uint64_t>(
    "SELECT_SENDER_GET_MORE_DATA_TIMEOUT");

  bool enhancedNetworkLogging = params.get<bool>("ENHANCED_NETWORK_LOGGING");

  SelectSender* sender = new SelectSender(
    phaseName, stageName, id, *sockets, numSenders, maxSendSize, selectTimeout,
    enhancedNetworkLogging, params);

  return sender;
}
