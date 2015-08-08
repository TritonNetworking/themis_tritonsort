#include "mapreduce/workers/sender/Sender.h"

Sender::Sender(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  SocketArray& sockets, uint64_t numSenders, uint64_t maxSendSize,
  bool enhancedNetworkLogging, const Params& params)
  : BaseSender(
    phaseName, stageName, id, sockets, numSenders, maxSendSize,
    enhancedNetworkLogging, params) {
}

void Sender::run() {
  bool needMoreWork = true;

  while (numCompletedPeers < connections.size()) {
    if (needMoreWork) {
      getMoreWork();
      needMoreWork = false;
    }

    // Iterate through all connections and send in round-robin order.
    for (ConnectionList::iterator iter = connections.begin();
         iter != connections.end(); iter++) {
      if (!(*iter)->socket.closed()) {
        if ((*iter)->buffer != NULL) {
          send(*iter);
        }

        // If we either didn't have a buffer, or we just finished sending one,
        // make sure to get another after this pass through the connections.
        if ((*iter)->buffer == NULL) {
          needMoreWork = true;
        }
      }
    }
  }
}

BaseWorker* Sender::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t maxSendSize = params.get<uint64_t>(
    "SEND_SOCKET_SYSCALL_SIZE");

  SocketArray* sockets = dependencies.get<SocketArray>(stageName, "sockets");

  uint64_t numSenders = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  bool enhancedNetworkLogging = params.get<bool>("ENHANCED_NETWORK_LOGGING");

  Sender* sender = new Sender(
    phaseName, stageName, id, *sockets, numSenders, maxSendSize,
    enhancedNetworkLogging, params);

  return sender;
}
