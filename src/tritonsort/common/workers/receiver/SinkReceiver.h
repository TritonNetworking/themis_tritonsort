#ifndef THEMIS_SINK_RECEIVER_H
#define THEMIS_SINK_RECEIVER_H

#include "common/workers/receiver/BaseReceiver.h"

/**
   SinkReceiver is a receiver worker implementation that performs non-blocking
   recv() calls in round-robin order across its connections but discards the
   results.
 */
class SinkReceiver : public BaseReceiver {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param name the worker stage name

     \param maxRecvSize maximum size of a single recv() call in bytes

     \param sockets an array of pre-opened receiver sockets

     \param numReceivers the total number of receiver workers

     \param enhancedNetworkLogging if true, log extra information about network
     connections
   */
  SinkReceiver(
    uint64_t id, const std::string& name, uint64_t maxRecvSize,
    SocketArray& sockets, uint64_t numReceivers, bool enhancedNetworkLogging);

  /// \warning Aborts. Do not call.
  BaseBuffer* newBuffer(uint8_t* metadata, uint64_t peerID);

private:
  /**
     Repeatedly performs non-blocking recv() calls in round-robin order across
     open connections. All received data is discarded.
   */
  virtual void receiverLoop();
};

#endif // THEMIS_SINK_RECEIVER_H
