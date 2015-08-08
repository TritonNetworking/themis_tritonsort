#ifndef THEMIS_BASE_RECEIVER_H
#define THEMIS_BASE_RECEIVER_H

#include <pthread.h>
#include <vector>

#include "core/Socket.h"
#include "core/SelfStartingWorker.h"

class BaseBuffer;

/**
   BaseReceiver is a class that encapsulates most of the receiver logic, but is
   not a complete worker implementation. Subclassing implementations are left to
   implement two methods:
     receiverLoop() - how the receiver actually services its connections - for
       example round-robin vs select()
     newBuffer() - construct a new buffer to receive into

   BaseReceiver implements non-blocking network recv() calls with error handling
   with the nonBlockingRecv() utility function. Subclasses can either call this
   directly, or use the default receive logic described in the next paragraph.
   When a connection is closed, nonBlockingRecv() will call closeConnection() if
   subclass wishes to add some custom close logic.

   BaseReceiver provides default receive logic with the receive() utility
   function. This function operates at the connection-level. If subclass
   specifies metadata, receive() will try to receive the appropriate amount of
   metadata before constructing a buffer with newBuffer(). Metadata can be used
   in buffer creation in BufferFactory::newInstanceFromNetwork().

   After metadata is received, receive() will try to receive the actual buffer
   contents, emitting when the buffer is full. At this point, it will expect
   another complete set of metadata bytes before creating a new buffer.

   This class prepares the following STL vectors (one item per connection)
   for use by implementations if they desire:
     buffers - buffer pointer to recv() into
     metadataBytesReceived - number of metadata bytes received
     metadataBuffers - raw uint8_t* buffer to store metadata
 */
class BaseReceiver : public SelfStartingWorker {
public:
  /// Constructor
  /**
     \param id the worker id

     \param name the worker stage name

     \param metadataSize the number of bytes of metadata to receive before
     receiving buffer contents

     \param maxRecvSize maximum size of a single recv() call in bytes

     \param sockets an array of pre-opened receiver sockets

     \param numReceivers the total number of receiver workers

     \param enhancedNetworkLogging if true, log extra information about network
     connections
   */
  BaseReceiver(
    uint64_t id, const std::string& name, const uint64_t metadataSize,
    uint64_t maxRecvSize, SocketArray& sockets, uint64_t numReceivers,
    bool enhancedNetworkLogging);

  /// Destructor
  virtual ~BaseReceiver();

  virtual StatLogger* initIntervalStatLogger();
  virtual void logIntervalStats(StatLogger& intervalLogger) const;

  /// Log statistics in teardown.
  void teardown();

  /**
     Sets up basic data structures and calls receiverLogic()
   */
  void run();

  /// Send number of bytes and buffers received so far to the resource monitor
  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
  */
  void resourceMonitorOutput(Json::Value& obj);

protected:
  /**
     Implements basic receive logic on a single connection. Tries to receive
     metadata first if complete metadata is not found. Then allocates a buffer
     using newBuffer() if one is not found, and receives into it.

     Most receivers will want to use this helper function, although some
     implementations, for example a sink receiver, may want to write their
     own receive logic.

     \param connectionID the connection to receive from
   */
  void receive(uint64_t connectionID);

  /**
     Issue a non-blocking recv() of a given size. Automatically handles error
     checking and updates internal state. This is automatically called in
     receive(), but is exposed in case an implementation wants to receive data
     without using BaseBuffers, for example a Sink receiver implemtantion.
     Automatically calls closeConnection() when a connection is closed.

     \param connectionID the connection to receive from

     \param buffer the buffer to receive into

     \param size the number of bytes to receive

     \return the number of bytes received, or 0 if no bytes were available or
     connection was closed
   */
  uint64_t nonBlockingRecv(
    uint64_t connectionID, uint8_t* buffer, uint64_t size);

  const uint64_t metadataSize;
  const uint64_t maxRecvSize;

  // Even though this class handles all socket logic, we might need the raw
  // sockets exposed to derived classes for select(), so leave this protected.
  SocketArray sockets;

  std::vector<BaseBuffer*> buffers;
  std::vector<uint64_t> metadataBytesReceived;
  std::vector<uint8_t*> metadataBuffers;

  mutable pthread_mutex_t connectionClosedLock;

  uint64_t totalBytesReceived;
  uint64_t totalBuffersReceived;
  uint64_t activeConnections;

  uint64_t receiveSizeStatID;
  StatLogger logger;

private:
  /**
     Derived classes should implement the main receiver logic in this function.
   */
  virtual void receiverLoop() = 0;

  /**
     Called by nonBlockingRecv() when a connection is closed. Any
     implementation-specific connection cleanup should be done here, for
     example clearing an fd set.

     \param socket the socket corresponding to the connection to be closed
   */
  virtual void closeConnection(Socket* socket) {
  }

  /**
     Construct a new buffer to receive into from metadata. Called by receive().

     \param metadata raw metadata bytes for the buffer to be created

     \param peerID the ID of the peer that sent this buffer

     \return a new buffer to receive into
   */
  virtual BaseBuffer* newBuffer(uint8_t* metadata, uint64_t peerID) = 0;

  const bool enhancedNetworkLogging;

  uint64_t blockedReceives;
  uint64_t totalReceives;

  std::vector<uint64_t> receiveQueueSizeStatIDs;
};

#endif // THEMIS_BASE_RECEIVER_H
