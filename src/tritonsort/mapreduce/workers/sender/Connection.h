#ifndef THEMIS_SENDER_CONNECTION_H
#define THEMIS_SENDER_CONNECTION_H

#include <list>

#include "core/Socket.h"
#include "core/StatLogger.h"
#include "core/Timer.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   The Connection class is a wrapper around a sender Socket that contains all
   connection logging infrastructure as well as network buffer and metadata
   information.
*/
class Connection {
public:
  /// Constructor
  /**
     \param socket the Socket that will actually send data

     \param senderLogger the sender's stat logger

     \param enhancedNetworkLogging if true, log extra information about network
     connections
   */
  Connection(
    Socket& socket, StatLogger& senderLogger, bool enhancedNetworkLogging);

  /// Destuctor
  virtual ~Connection();

  /// Close the connection
  void close();

  /// Register connection-related stats with the interval logger
  /**
     \param intervalLogger the stat logger used for interval logging
   */
  void registerWithIntervalLogger(StatLogger& intervalLogger);

  /// Log connection-related stats with the interval logger
  /**
     \param intervalLogger the stat logger used for interval logging
   */
  void logIntervalStats(StatLogger& intervalLogger);

  const bool enhancedNetworkLogging;

  Socket& socket;

  KVPairBuffer* buffer;
  KVPairBuffer::NetworkMetadata* metadata;
  uint64_t metadataBytesSent;
  uint64_t bufferBytesSent;
  uint64_t totalBytesSent;

  bool broken;

  std::string loggingSuffix;

private:
  Timer connectionTimer;
  StatLogger& senderLogger;

  uint64_t bytesSentStatID;
  uint64_t congestionWindowStatID;
  uint64_t retransmitsStatID;
  uint64_t rttStatID;
  uint64_t rttVarianceStatID;
  uint64_t senderSlowStartThresholdStatID;
  uint64_t lastDataSentStatID;
  uint64_t sendQueueSizeStatID;
};

typedef std::list<Connection*> ConnectionList;

#endif // THEMIS_SENDER_CONNECTION_H
