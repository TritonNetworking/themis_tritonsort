#ifndef THEMIS_RECEIVER_H
#define THEMIS_RECEIVER_H

#include "common/workers/receiver/BaseReceiver.h"

/**
   Receiver is a receiver worker implementation that repeatedly performs
   non-blocking recv() calls in round-robin order across its open connections.
 */
template <typename OutFactory> class Receiver : public BaseReceiver {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param name the worker stage name

     \param maxRecvSize maximum size of a single recv() call in bytes

     \param memoryAllocator the allocator used to construct new buffers

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param sockets an array of pre-opened receiver sockets

     \param numReceivers the total number of receiver workers

     \param enhancedNetworkLogging if true, log extra information about network
     connections
   */
  Receiver(
    uint64_t id, const std::string& name, uint64_t maxRecvSize,
    MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize,
    SocketArray& sockets, uint64_t numReceivers, bool enhancedNetworkLogging)
    : BaseReceiver(
      id, name, OutFactory::networkMetadataSize(), maxRecvSize, sockets,
      numReceivers, enhancedNetworkLogging),
      bufferFactory(
        *this, memoryAllocator, 0, alignmentSize) {
  }

private:
  /**
     Repeatedly performs non-blocking recv() calls in round-robin order across
     open connections.
   */
  void receiverLoop() {
    // Loop through active connections trying to do non-blocking recv() calls
    while (this->activeConnections > 0) {
      for (uint64_t i = 0; i < this->sockets.size(); i++) {
        // If this connection was closed, skip it
        if (this->sockets.at(i)->closed()) {
          continue;
        }

        // Receive metadata and then actual data for this connection.
        this->receive(i);
      }
    }
  }

  /**
     Create a new network-sourced buffer using metadata and peerID information
     as needed. If OutFactory does not implement this method, a default sized
     buffer will be constructed as designated in
     BufferFactory::newInstanceFromNetwork()

     \param metadata raw metadata bytes for the buffer to be created

     \param peerID the ID of the peer that sent this buffer

     \return a new buffer to receive into
   */
  BaseBuffer* newBuffer(uint8_t* metadata, uint64_t peerID) {
    return bufferFactory.newInstanceFromNetwork(metadata, peerID);
  }

  OutFactory bufferFactory;
};

template <typename OutFactory>
BaseWorker* Receiver<OutFactory>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // Maximize size of an individual recv() call when receiving buffer contents.
  uint64_t maxRecvSize = params.get<uint64_t>("RECV_SOCKET_SYSCALL_SIZE");

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  SocketArray* sockets = dependencies.get<SocketArray>(stageName, "sockets");

  uint64_t numReceivers = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  bool enhancedNetworkLogging = params.get<bool>("ENHANCED_NETWORK_LOGGING");

  Receiver<OutFactory>* receiver = new Receiver<OutFactory>(
    id, stageName, maxRecvSize, memoryAllocator, alignmentSize,
    *sockets, numReceivers, enhancedNetworkLogging);

  return receiver;
}

#endif // THEMIS_RECEIVER_H
