#ifndef THEMIS_SELECT_RECEIVER_H
#define THEMIS_SELECT_RECEIVER_H

#include <sys/select.h>

#include "common/workers/receiver/BaseReceiver.h"

/**
   SelectReceiver is a receiver worker implementation that uses a blocking
   select() call to wait until some sockets have data ready. Once the select()
   returns, SelectReceiver uses standard receive logic defined in BaseReceiver.
 */
template <typename OutFactory> class SelectReceiver : public BaseReceiver {
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
  SelectReceiver(
    uint64_t id, const std::string& name, uint64_t maxRecvSize,
    MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize,
    SocketArray& sockets, uint64_t numReceivers, bool enhancedNetworkLogging)
    : BaseReceiver(
      id, name, OutFactory::networkMetadataSize(), maxRecvSize, sockets,
      numReceivers, enhancedNetworkLogging),
      bufferFactory(*this, memoryAllocator, 0, alignmentSize) {
  }

private:
  /**
     Wait on a blocking select() until some sockets are ready. Then receive from
     ready sockets and go back to select.
   */
  void receiverLoop() {
    fd_set readDescriptors;

    FD_ZERO(&masterDescriptorSet);
    FD_ZERO(&readDescriptors);

    // Initialize maxFD as the largest descriptor in the socket list, and set
    // the master FD set
    maxFD = -1;
    for (SocketArray::iterator iter = this->sockets.begin();
         iter != this->sockets.end(); iter++) {
      maxFD = std::max((*iter)->getFD(), maxFD);
      FD_SET((*iter)->getFD(), &masterDescriptorSet);
    }

    while (this->activeConnections > 0) {
      // Put active sockets into readDescriptors
      readDescriptors = masterDescriptorSet;

      // Select on all active sockets, waiting indefinitely for one of them to
      // become available for reading
      int status = select(maxFD + 1, &readDescriptors, NULL, NULL, NULL);
      ABORT_IF(status == -1, "select() failed with error %d: %s",
               errno, strerror(errno));
      // Since status == 0 only when timeouts expire, it ought to be safe for
      // to make this an assert rather than an abort.
      TRITONSORT_ASSERT(status != 0, "Unexpected select() return code 0: no data "
             "available despite no timeout having been set");

      // Read from each available socket
      for (uint64_t connectionID = 0; connectionID < this->sockets.size();
           connectionID++) {
        if (this->sockets.at(connectionID)->closed() || !FD_ISSET(
              this->sockets.at(connectionID)->getFD(), &readDescriptors)) {
          // No data for this connection, skip it.
          continue;
        }

        this->receive(connectionID);
      }
    }
  }

  /**
     Update fd sets and maxFD value when connections are closed.

     \param socket the socket corresponding to the connection to be closed
   */
  void closeConnection(Socket* socket) {
    // Remove from master fd set
    FD_CLR(socket->getFD(), &masterDescriptorSet);

    // Update maxFD so select has a correct range
    maxFD = -1;
    for (SocketArray::iterator iter = this->sockets.begin();
         iter != this->sockets.end(); iter++) {
      // Check all *other* open sockets
      if ((*iter) != socket && !(*iter)->closed()) {
        maxFD = std::max(maxFD, (*iter)->getFD());
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

  fd_set masterDescriptorSet;
  int maxFD;

  OutFactory bufferFactory;
};

template <typename OutFactory>
BaseWorker* SelectReceiver<OutFactory>::newInstance(
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

  SelectReceiver<OutFactory>* receiver = new SelectReceiver<OutFactory>(
    id, stageName, maxRecvSize, memoryAllocator, alignmentSize, *sockets,
    numReceivers, enhancedNetworkLogging);

  return receiver;
}

#endif // THEMIS_SELECT_RECEIVER_H
