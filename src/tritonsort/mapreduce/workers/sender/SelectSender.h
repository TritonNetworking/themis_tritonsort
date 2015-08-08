#ifndef THEMIS_MAPRED_SELECT_SENDER_H
#define THEMIS_MAPRED_SELECT_SENDER_H

#include "mapreduce/workers/sender/BaseSender.h"

/**
   SelectSender sends data to receiving peers only if their sockets can accept
   data. The select() call is used to ensure this property, and file descriptor
   sets are maintained as sockets get new buffers and empty existing buffers.

   Because some sockets might not have buffers at the time of the select() call,
   a timeout, specified in microseconds by SELECT_SENDER_GET_MORE_DATA_TIMEOUT,
   is used to allow the SelectSender to have a chance to get more data for the
   sockets that don't have any to send. However, if all sockets do in fact have
   data, then select() is issued without a timeout.
 */
class SelectSender : public BaseSender {
WORKER_IMPL

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

     \param selectTimeout the timeout for the select() call in microseconds

     \param enhancedNetworkLogging if true, log extra information about network
     connections

     \param params a Params object that this worker will use to initialize its
     coordinator client
  */
  SelectSender(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    SocketArray& sockets, uint64_t numSenders, uint64_t maxSendSize,
    uint64_t selectTimeout, bool enhancedNetworkLogging, const Params& params);

  /// Repeatedly select() on sockets with data until done sending to ensure that
  /// we don't waste time on send() calls that would have blocked otherwise.
  void run();

private:
  /**
     Add this connection to the fd set since we can send data.

     \param connection the connection that got a new buffer
   */
  void handleNewBuffer(Connection* connection);

  /**
     Remove this connection from the fd set since we can't send data.

     \param connection the connection that just emptied its buffer
   */
  void handleEmptyBuffer(Connection* connection);

  const uint64_t selectTimeoutMicros;

  fd_set socketsWithData;

  uint64_t numSocketsWithData;
  int maxFD;
};

#endif // THEMIS_MAPRED_SELECT_SENDER_H
