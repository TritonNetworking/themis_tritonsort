#ifndef THEMIS_MAPRED_BASE_SENDER_H
#define THEMIS_MAPRED_BASE_SENDER_H

#include "core/MultiQueueRunnable.h"
#include "core/Socket.h"
#include "core/StatLogger.h"
#include "core/Timer.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/sender/Connection.h"

/**
   BaseSender is a base class that holds all sender logic related to sockets and
   sending individual buffers. Subclasses can use this connection information to
   send data by calling send(connection). The idea is that the subclass can
   choose the policy by which sends are issued, for example round robin or after
   a select.
 */
class BaseSender : public MultiQueueRunnable<KVPairBuffer> {
public:
  /// Constructor
  /**
     \param phaseName the name of the phase in which this worker is running

     \param stageName the name of the stage for which this worker is working

     \param id the stage's ID

     \param sockets an array of pre-opened sender sockets

     \param numSenders the total number of sender workers

     \param maxSendSize the maximum size of a non-blocking send() syscall in
     bytes

     \param enhancedNetworkLogging if true, log extra information about network
     connections

     \param params a Params object that this worker will use to initialize its
     coordinator client
  */
  BaseSender(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    SocketArray& sockets, uint64_t numSenders, uint64_t maxSendSize,
    bool enhancedNetworkLogging, const Params& params);

  /// Destructor
  virtual ~BaseSender();

  void resourceMonitorOutput(Json::Value& obj);

  virtual StatLogger* initIntervalStatLogger();
  virtual void logIntervalStats(StatLogger& intervalLogger) const;

  /// Close connections to all destination peers and ensure that no buffers
  /// remain unsent.
  void teardown();

protected:
  /**
     Get more buffers from the tracker for each connection that currently
     has no buffer to send.
   */
  void getMoreWork();

  /**
     Send some bytes on a connection in a non-blocking fashion. Metadata
     is sent first, and then the buffer contents after all metadata has been
     sent. All cleanup and connection failure handling is taken care of by
     this method.

     \param connection the connection to send on
   */
  void send(Connection* connection);

  ConnectionList connections;
  uint64_t numCompletedPeers;

private:
  /**
     Helper function to send a byte array on a connection.

     \param connection the connection to send on

     \param data the bytes to send

     \param bytesToSend the number of bytes to send
   */
  ssize_t sendData(Connection* connection, uint8_t* data, uint64_t bytesToSend);

  /**
     Called by getMoreWork() when a new buffer is retrieved for a connection.
     Any implementation-specific work should be done here, for example updating
     fd sets.

     \param connection the connection that got a new buffer
   */
  virtual void handleNewBuffer(Connection* connection) {
  }

  /**
     Called by send() when a connection finishes sending a buffer. Any
     implementation-specific work should be done here, for example updating fd
     sets.

     \param connection the connection that just emptied its buffer
   */
  virtual void handleEmptyBuffer(Connection* connection) {
  }

  CoordinatorClientInterface* coordinatorClient;

  mutable pthread_mutex_t connectionClosedLock;

  uint64_t numIdleSockets;
  uint64_t totalBytesSent;

  uint64_t blockedSends;
  uint64_t totalSends;

  uint64_t maxSendSize;

  uint64_t totalBytesSentStatID;
  uint64_t numIdleSocketsStatID;

  StatLogger logger;

  // BaseSender managers everything through Connections, but we'll store the
  // socket array simply so we can clean up memory at destruction time.
  SocketArray sockets;
};

#endif // THEMIS_MAPRED_BASE_SENDER_H
