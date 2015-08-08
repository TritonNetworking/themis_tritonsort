#ifndef THEMIS_MAPRED_SENDER_H
#define THEMIS_MAPRED_SENDER_H

#include "mapreduce/workers/sender/BaseSender.h"

/**
   Sender is a worker that sends buffers to different receiving peers in a
   round-robin non-blocking fashion. Because there is no select() to see if
   the peer can actually receive data, it should be expected that many of
   the send() calls will return 0 bytes, which will be logged as the datum
   "would_have_blocked".
 */
class Sender : public BaseSender {
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

     \param enhancedNetworkLogging if true, log extra information about network
     connections

     \param params a Params object that this worker will use to initialize its
     coordinator client
  */
  Sender(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    SocketArray& sockets, uint64_t numSenders, uint64_t maxSendSize,
    bool enhancedNetworkLogging, const Params& params);

  /// Repeatedly visit all sockets in round-robin order until done sending.
  void run();
};

#endif // THEMIS_MAPRED_SENDER_H
