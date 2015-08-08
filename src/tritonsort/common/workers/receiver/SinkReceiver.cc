#include "common/workers/receiver/SinkReceiver.h"
#include "core/MemoryUtils.h"

SinkReceiver::SinkReceiver(
  uint64_t id, const std::string& name, uint64_t maxRecvSize,
  SocketArray& sockets, uint64_t numReceivers, bool enhancedNetworkLogging)
  : BaseReceiver(
    id, name, 0, maxRecvSize, sockets, numReceivers, enhancedNetworkLogging) {
}

void SinkReceiver::receiverLoop() {
  uint8_t* buffer = new (themis::memcheck) uint8_t[maxRecvSize];

  while (activeConnections > 0) {
    for (uint64_t i = 0; i < sockets.size(); i++) {
      // If this connection was closed, skip it
      if (sockets.at(i)->closed()) {
        continue;
      }

      // Receive bytes and throw them on the floor.
      uint64_t bytesReceived = nonBlockingRecv(i, buffer, maxRecvSize);
      logger.add(receiveSizeStatID, bytesReceived);
      totalBytesReceived += bytesReceived;
      totalBuffersReceived++;
      // Since the sink receiver doesn't produce buffers, just log that we
      // consumed a single work unit of size bytesReceived
      logConsumed(1, bytesReceived);
    }
  }

  delete[] buffer;
}

BaseBuffer* SinkReceiver::newBuffer(uint8_t* metadata, uint64_t peerID) {
  ABORT("SinkReceiver should not call newBuffer()");
  return NULL;
}

BaseWorker* SinkReceiver::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // Maximize size of an individual recv() call when receiving buffer contents.
  uint64_t maxRecvSize = params.get<uint64_t>("RECV_SOCKET_SYSCALL_SIZE");

  SocketArray* sockets = dependencies.get<SocketArray>(stageName, "sockets");

  uint64_t numReceivers = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  bool enhancedNetworkLogging = params.get<bool>("ENHANCED_NETWORK_LOGGING");

  SinkReceiver* receiver = new SinkReceiver(
    id, stageName, maxRecvSize, *sockets, numReceivers, enhancedNetworkLogging);

  return receiver;
}
