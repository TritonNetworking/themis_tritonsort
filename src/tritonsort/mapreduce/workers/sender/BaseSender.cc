#include <sys/socket.h>
#include <sys/types.h>

#include "core/StatusPrinter.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/workers/sender/BaseSender.h"

BaseSender::BaseSender(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  SocketArray& _sockets, uint64_t numSenders, uint64_t _maxSendSize,
  bool enhancedNetworkLogging, const Params& params)
  : MultiQueueRunnable<KVPairBuffer>(id, stageName),
    numCompletedPeers(0),
    coordinatorClient(NULL),
    numIdleSockets(0),
    totalBytesSent(0),
    blockedSends(0),
    totalSends(0),
    maxSendSize(_maxSendSize),
    totalBytesSentStatID(0),
    numIdleSocketsStatID(0),
    logger(stageName, id) {
  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, phaseName, stageName, id);
  pthread_mutex_init(&connectionClosedLock, NULL);

  // Pick off the portion of sockets this sender is responsible for.
  uint64_t socketsPerSender = _sockets.size() / numSenders;
  uint64_t remainder = _sockets.size() % numSenders;
  uint64_t startSocket = socketsPerSender * id;
  // Allocate the remainder 1 socket per sender until we run out
  if (remainder >= id) {
    // Every sender before this one got some remainder.
    startSocket += id;
    remainder -= id;
  } else {
    // We used up all the remainder prior to this sender
    startSocket += remainder;
    remainder = 0;
  }

  uint64_t endSocket = startSocket + socketsPerSender;
  if (remainder > 0) {
    endSocket++;
  }

  // Wrap our sockets with Connection objects.
  for (SocketArray::iterator iter = _sockets.begin() + startSocket;
       iter != _sockets.begin() + endSocket; iter++) {
    Connection* connection = new Connection(
      *(*iter), logger, enhancedNetworkLogging);
    connections.push_back(connection);
  }

  // Store a list of the actual sockets so we can clean them up later
  sockets.assign(
    _sockets.begin() + startSocket, _sockets.begin() + endSocket);
}

BaseSender::~BaseSender() {
  if (coordinatorClient != NULL) {
    delete coordinatorClient;
    coordinatorClient = NULL;
  }

  // Clean up connections.
  for (ConnectionList::iterator iter = connections.begin();
       iter != connections.end(); iter++) {
    delete (*iter);
  }

  // Clean up sockets.
  for (SocketArray::iterator iter = sockets.begin(); iter != sockets.end();
       iter++) {
    delete (*iter);
  }

  pthread_mutex_destroy(&connectionClosedLock);
}

void BaseSender::resourceMonitorOutput(
  Json::Value& obj) {
  MultiQueueRunnable<KVPairBuffer>::resourceMonitorOutput(obj);
  obj["bytes_sent"] = Json::UInt64(totalBytesSent);

  for (ConnectionList::iterator iter = connections.begin();
       iter!= connections.end(); iter++) {
    obj["bytes_sent" + (*iter)->loggingSuffix] =
      Json::UInt64((*iter)->totalBytesSent);
  }
}

StatLogger* BaseSender::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger(getName(), getID());

  totalBytesSentStatID = intervalLogger->registerStat("total_bytes_sent");
  numIdleSocketsStatID = intervalLogger->registerStat("num_idle_sockets");

  for (ConnectionList::iterator iter = connections.begin();
       iter!= connections.end(); iter++) {
    (*iter)->registerWithIntervalLogger(*intervalLogger);
  }

  return intervalLogger;
}

void BaseSender::logIntervalStats(StatLogger& intervalLogger) const {
  // To prevent connections from closing while we're logging, take a mutex.
  pthread_mutex_lock(&connectionClosedLock);

  for (ConnectionList::const_iterator iter = connections.begin();
       iter!= connections.end(); iter++) {
    (*iter)->logIntervalStats(intervalLogger);
  }

  pthread_mutex_unlock(&connectionClosedLock);

  intervalLogger.add(totalBytesSentStatID, totalBytesSent);
  intervalLogger.add(numIdleSocketsStatID, numIdleSockets);
}

void BaseSender::teardown() {
  StatusPrinter::add("%s %llu closing all connections",
                     getName().c_str(), getID());

  // Close all open connections.
  for (ConnectionList::iterator iter = connections.begin();
       iter != connections.end(); iter++) {
    if (!(*iter)->socket.closed()) {
      (*iter)->close();
    }

    // Sanity check
    ASSERT((*iter)->buffer == NULL, "Somehow we still have a buffer at "
           "teardown for peer %llu", (*iter)->socket.getPeerID());
  }

  StatusPrinter::add("%s %llu closed all connections",
                     getName().c_str(), getID());

  logger.logDatum("would_have_blocked", blockedSends);
  logger.logDatum("total_ios", totalSends);
}

void BaseSender::getMoreWork() {
  bool senderHasWork = false;

  numIdleSockets = 0;
  for (ConnectionList::iterator iter = connections.begin();
       iter != connections.end(); iter++) {
    if (!(*iter)->socket.closed() && (*iter)->buffer == NULL) {
      // We need a new buffer for this connection.
      if (attemptGetNewWork((*iter)->socket.getPeerID(), (*iter)->buffer)) {
        // We got a work unit for this peer.
        if ((*iter)->buffer == NULL) {
          // To prevent the interval logger from calling ioctl on sockets while
          // we're closing them, take a mutex.
          pthread_mutex_lock(&connectionClosedLock);

          // But the work unit was NULL, which means we're done sending to this
          // peer, so close the connection.
          StatusPrinter::add(
            "%s %llu closing flow %llu to peer %llu", getName().c_str(),
            getID(), (*iter)->socket.getFlowID(), (*iter)->socket.getPeerID());
          (*iter)->close();
          numCompletedPeers++;

          pthread_mutex_unlock(&connectionClosedLock);
        } else {
          // There is at least one new piece of work, so the Sender is no longer
          // blocked on work units.
          senderHasWork = true;

          // If the connection was broken, just ignore this buffer, but delete
          // to prevent a memory leak.
          /// \TODO(MC): Ideally we could just send this data on one of the
          /// other connections to this peer, but since the fault tolerance
          /// branch was never fully integrated, don't bother.
          if ((*iter)->broken) {
            delete (*iter)->buffer;
            (*iter)->buffer = NULL;
          } else {
            // Fill in metadata and reset number of bytes sent.
            (*iter)->metadata = (*iter)->buffer->getNetworkMetadata();
            (*iter)->metadataBytesSent = 0;
            (*iter)->bufferBytesSent = 0;

            handleNewBuffer(*iter);
          }
        }
      } else {
        numIdleSockets++;
      }
    } else if (!(*iter)->socket.closed() && (*iter)->buffer != NULL) {
      // We already have a work unit for this connection.
      senderHasWork = true;
    }
  }

  if (senderHasWork) {
    // We have at least one work unit so we're not idle.
    stopWaitForWorkTimer();
  } else {
    // We have no work units, so we are idle.
    startWaitForWorkTimer();
  }
}

void BaseSender::send(Connection* connection) {
  ABORT_IF(connection->buffer == NULL, "Tried to send to peer %llu flow "
           "%llu but buffer is NULL.", connection->socket.getPeerID(),
           connection->socket.getFlowID());

  if (connection->metadata != NULL) {
    // We still have to send the metadata. Issue a non-blocking send for the
    // remaining number of metadata bytes.
    uint64_t bytesRemaining = sizeof(KVPairBuffer::NetworkMetadata) -
      connection->metadataBytesSent;
    ASSERT(bytesRemaining != 0,
           "No metadata bytes to send but metadata not NULL");

    ssize_t bytesSent = sendData(
      connection,
      (uint8_t*) connection->metadata + connection->metadataBytesSent,
      bytesRemaining);

    if (bytesSent <= 0) {
      // Either there was an error, or we couldn't send any data, so just
      // return.
      return;
    }

    // Update statistics
    connection->metadataBytesSent += bytesSent;
    connection->totalBytesSent += bytesSent;
    totalBytesSent += bytesSent;

    if (connection->metadataBytesSent <
        sizeof(KVPairBuffer::NetworkMetadata)) {
      // We still have more metadata to send, so return for now.
      return;
    }

    // We finished sending all the metadata.
    delete connection->metadata;
    connection->metadata = NULL;
  }

  // Now send the buffer.
  uint64_t bytesRemaining = connection->buffer->getCurrentSize() -
    connection->bufferBytesSent;
  ASSERT(bytesRemaining != 0, "No buffer bytes to send.");

  // Only send() up to maxSendSize even if we can send more.
  bytesRemaining = std::min(bytesRemaining, maxSendSize);

  ssize_t bytesSent = sendData(
    connection,
    (uint8_t*) connection->buffer->getRawBuffer() + connection->bufferBytesSent,
    bytesRemaining);

  if (bytesSent > 0) {
    // Update statistics
    connection->bufferBytesSent += bytesSent;
    connection->totalBytesSent += bytesSent;
    totalBytesSent += bytesSent;

    if (connection->bufferBytesSent == connection->buffer->getCurrentSize()) {
      // We finished sending the buffer.
      delete connection->buffer;
      connection->buffer = NULL;

      handleEmptyBuffer(connection);
    }
  }
}

ssize_t BaseSender::sendData(
  Connection* connection, uint8_t* data, uint64_t bytesToSend) {
  ssize_t bytesSent = ::send(
    connection->socket.getFD(), data, bytesToSend, MSG_DONTWAIT);
  totalSends++;
  if (bytesSent == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      // This send call would have blocked normally, so update statistics.
      blockedSends++;
      return 0;
    } else {
      // The send call actually failed.
      StatusPrinter::add(
        "send() to peer %llu, flow %llu failed with error %d: %s",
        connection->socket.getPeerID(), connection->socket.getFlowID(), errno,
        strerror(errno));

      // Inform the coordinator that this connection is broken.
      if (coordinatorClient != NULL) {
        const std::string& failedIPAddress = connection->socket.getAddress();
        coordinatorClient->notifyNodeFailure(failedIPAddress);
      } else {
        // Don't silently ignore failures if no coordinator client was set.
        ABORT("send() to peer %llu, flow %llu failed with error %d: %s",
              connection->socket.getPeerID(), connection->socket.getFlowID(),
              errno, strerror(errno));
      }

      // Make sure we mark the connection as broken so we don't send more
      // data to it.
      connection->broken = true;

      // Delete metadata and buffer associated with this connection.
      if (connection->metadata != NULL) {
        delete connection->metadata;
        connection->metadata = NULL;
      }
      delete connection->buffer;
      connection->buffer = NULL;

      handleEmptyBuffer(connection);
    }
  }

  return bytesSent;
}
