#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "common/BufferFactory.h"
#include "common/buffers/BaseBuffer.h"
#include "common/workers/receiver/BaseReceiver.h"
#include "core/MemoryUtils.h"

BaseReceiver::BaseReceiver(
  uint64_t id, const std::string& name, uint64_t _metadataSize,
  uint64_t _maxRecvSize, std::vector<Socket*>& _sockets, uint64_t numReceivers,
  bool _enhancedNetworkLogging)
  : SelfStartingWorker(id, name),
    metadataSize(_metadataSize),
    maxRecvSize(_maxRecvSize),
    totalBytesReceived(0),
    totalBuffersReceived(0),
    activeConnections(0),
    logger(name, id),
    enhancedNetworkLogging(_enhancedNetworkLogging),
    blockedReceives(0),
    totalReceives(0) {
  // Pick off the portion of sockets this receiver is responsible for.
  uint64_t socketsPerReceiver = _sockets.size() / numReceivers;
  uint64_t remainder = _sockets.size() % numReceivers;
  uint64_t startSocket = socketsPerReceiver * id;
  // Allocate the remainder 1 socket per receiver until we run out
  if (remainder >= id) {
    // Every receiver before this one got some remainder.
    startSocket += id;
    remainder -= id;
  } else {
    // We used up all the remainder prior to this receiver
    startSocket += remainder;
    remainder = 0;
  }

  uint64_t endSocket = startSocket + socketsPerReceiver;
  if (remainder > 0) {
    endSocket++;
  }

  sockets.assign(
    _sockets.begin() + startSocket, _sockets.begin() + endSocket);

  // receive() is responsible for logging this stat.
  receiveSizeStatID = logger.registerHistogramStat("receive_size", 10000);
  pthread_mutex_init(&connectionClosedLock, NULL);
}

BaseReceiver::~BaseReceiver() {
  for (std::vector<uint8_t*>::iterator iter = metadataBuffers.begin();
       iter != metadataBuffers.end(); iter++) {
    if (*iter != NULL) {
      delete[] *iter;
      *iter = NULL;
    }
  }

  // Clean up sockets.
  for (SocketArray::iterator iter = sockets.begin(); iter != sockets.end();
       iter++) {
    delete (*iter);
  }

  pthread_mutex_destroy(&connectionClosedLock);
}

StatLogger* BaseReceiver::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger(getName(), getID());

  if (enhancedNetworkLogging) {
    for (uint64_t i = 0; i < sockets.size(); i++) {
      receiveQueueSizeStatIDs.push_back(
        intervalLogger->registerStat(
          "receive_queue_size_" + boost::lexical_cast<std::string>(i)));
    }
  }

  return intervalLogger;
}

void BaseReceiver::logIntervalStats(StatLogger& intervalLogger) const {
  if (enhancedNetworkLogging) {
    // To prevent connections from closing while we're logging, take a mutex.
    pthread_mutex_lock(&connectionClosedLock);

    uint64_t socketID = 0;
    for (SocketArray::const_iterator iter = sockets.begin();
         iter!= sockets.end(); iter++, socketID++) {
      if (!(*iter)->closed()) {
        // Get the number of unread bytes in the receive queue.
        int unreadBytes = 0;
        int status = ioctl((*iter)->getFD(), FIONREAD, &unreadBytes);
        ABORT_IF(status != 0, "ioctl(FIONREAD) failed with errno %d: %s",
                 errno, strerror(errno));
        intervalLogger.add(receiveQueueSizeStatIDs[socketID], unreadBytes);
      }
    }

    pthread_mutex_unlock(&connectionClosedLock);
  }
}

void BaseReceiver::teardown() {
  logger.logDatum("would_have_blocked", blockedReceives);
  logger.logDatum("total_ios", totalReceives);
}

void BaseReceiver::run() {
  // Because a receiver sources its data from the network rather than from
  // tracker queues, idleness cannot be set at a fine granularity (normally
  // between run() invocations for SingleUnitRunnables when tracker queues are
  // empty). In order to allow the deadlock detector to find a deadlock
  // elsewhere in the pipeline, we cheat and tell it the receiver is always
  // idle.
  setIdle(true);

  uint64_t numConnections = sockets.size();

  // In the event that this node's receiver-side pipeline has been disabled, for
  // example in the case of a merge pipeline, just exit.
  if (numConnections == 0) {
    StatusPrinter::add(
      "%s %llu has been short-circuited", getName().c_str(), getID());
  } else {
    // Initially we don't have buffers for any peer because we're waiting on
    // metadata.
    buffers.resize(numConnections, NULL);
    metadataBytesReceived.resize(numConnections, 0);

    for (uint64_t i = 0; i < numConnections; i++) {
      // The application-specific receiver implementation determines how large
      // metadata buffers are.
      uint8_t* metadata = NULL;
      if (metadataSize > 0) {
        metadata = new (themis::memcheck) uint8_t[metadataSize];
      }
      metadataBuffers.push_back(metadata);

      // Additionally sanity check sockets
      TRITONSORT_ASSERT(sockets.at(i)->getFD() >= 0,
             "Got bad socket descriptor %d for connection %llu",
             sockets.at(i)->getFD(), i);
    }

    activeConnections = numConnections;
    receiverLoop();

    // receive() is responsible for calculating totalBytesReceived
    logInputSize(totalBytesReceived);
  }
}

void BaseReceiver::resourceMonitorOutput(Json::Value& obj) {
  SelfStartingWorker::resourceMonitorOutput(obj);
  obj["bytes_received"] = Json::UInt64(totalBytesReceived);
  obj["buffers_received"] = Json::UInt64(totalBuffersReceived);
  obj["active_connections"] = Json::UInt64(activeConnections);
}

void BaseReceiver::receive(uint64_t connectionID) {
  uint64_t& numMetadataBytesReceived = metadataBytesReceived.at(connectionID);
  BaseBuffer*& buffer = buffers.at(connectionID);
  uint8_t*& metadataBuffer = metadataBuffers.at(connectionID);

  // Try to receive metadata if we need more metadata.
  if (numMetadataBytesReceived < metadataSize) {
    // We have more metadata to receive, make sure we don't have a buffer.
    TRITONSORT_ASSERT(buffer == NULL, "Need metadata from connection %llu but a buffer "
           "exists.", connectionID);

    // Issue a nonblocking receive into the metadata buffer for this connection.
    uint8_t* recvBuffer = metadataBuffer + numMetadataBytesReceived;
    uint64_t size = metadataSize - numMetadataBytesReceived;

    uint64_t bytesReceived = nonBlockingRecv(connectionID, recvBuffer, size);

    if (bytesReceived == 0) {
      return;
    }

    // Received metadata bytes normally.
    numMetadataBytesReceived += bytesReceived;
    logger.add(receiveSizeStatID, bytesReceived);
    totalBytesReceived += bytesReceived;
    if (numMetadataBytesReceived != metadataSize) {
      // Still waiting on more metadata.
      return;
    }
  }

  // Reaching this point means we have complete metaata.
  TRITONSORT_ASSERT(numMetadataBytesReceived == metadataSize, "Should have complete "
         "metadata for connection %d but only have %llu/%llu bytes.",
         numMetadataBytesReceived, metadataSize);

  // Get a new buffer if we don't have one.
  if (buffer == NULL) {
    buffer = newBuffer(metadataBuffer, sockets.at(connectionID)->getPeerID());
  }

  // Issue non-blocking recv() into the actual output buffer, using
  // setupAppend/commitAppend to access the raw, underlying buffer.
  uint64_t size = buffer->getCapacity() - buffer->getCurrentSize();
  const uint8_t* appendPtr = buffer->setupAppend(size);

  uint64_t bytesReceived = nonBlockingRecv(
    connectionID, const_cast<uint8_t*>(appendPtr), size);

  if (bytesReceived == 0) {
    buffer->abortAppend(appendPtr);
  } else {
    buffer->commitAppend(appendPtr, bytesReceived);
  }

  logger.add(receiveSizeStatID, bytesReceived);
  totalBytesReceived += bytesReceived;

  // Check to see if the recv() call filled up the buffer.
  if (buffer->full() || sockets.at(connectionID)->closed()) {
    // Update statistics.
    totalBuffersReceived++;
    logConsumed(buffer);

    // Emit the buffer.
    emitWorkUnit(buffer);
    buffer = NULL;

    // Ask for another metadata buffer.
    numMetadataBytesReceived = 0;
  }
}

uint64_t BaseReceiver::nonBlockingRecv(
  uint64_t connectionID, uint8_t* buffer, uint64_t size) {
  Socket*& socket = sockets.at(connectionID);

  TRITONSORT_ASSERT(!socket->closed(),
         "Tried to receive from a closed connection %llu", connectionID);

  TRITONSORT_ASSERT(size > 0, "Tried to receive 0 bytes.");

  // Only recv() up to maxRecvSize even if we can receive more.
  size = std::min(size, maxRecvSize);

  // Issue a non-blocking recv() call into the specified buffer.
  ssize_t bytesReceived = recv(socket->getFD(), buffer, size, MSG_DONTWAIT);
  totalReceives++;

  bool closeSocket = false;

  if (bytesReceived == 0) {
    // Connection was shut down in an orderly fashion.
    StatusPrinter::add(
      "%s %llu orderly shutodwn connection %d, socket %d",
      getName().c_str(), getID(), connectionID, socket->getFD());
    closeSocket = true;
  } else if (bytesReceived < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      // Not really an error. This peer didn't have anything to send.
      blockedReceives++;
      return 0;
    } else {
      // Socket broke; log it and stop receiving, but don't die
      StatusPrinter::add(
        "Connection %llu, socket %d recv() returned errno %d : %s",
        connectionID, socket->getFD(), errno, strerror(errno));
      BaseBuffer*& buffer = buffers.at(connectionID);

      // Don't emit this socket's buffer, since it might contain
      // partially-received garbage
      if (buffer != NULL) {
        delete buffer;
        buffer = NULL;
      }

      closeSocket = true;
    }
  }

  if (closeSocket) {
    // To prevent the interval logger from calling ioctl on sockets while we're
    // closing them, take a mutex.
    pthread_mutex_lock(&connectionClosedLock);

    StatusPrinter::add(
      "%s %llu closing connection %d, socket %d", getName().c_str(), getID(),
      connectionID, socket->getFD());

    // Perform any implementation-specific handling.
    closeConnection(socket);

    // Close the socket.
    socket->close();

    activeConnections--;

    pthread_mutex_unlock(&connectionClosedLock);

    return 0;
  }

  // recv() completed normally. Return the number of bytes received.
  TRITONSORT_ASSERT(bytesReceived <= static_cast<int64_t>(size), "Read more bytes than we "
         "wanted (%d > %llu)", bytesReceived, size);
  return bytesReceived;
}
