#ifndef THEMIS_SOCKET_H
#define THEMIS_SOCKET_H

#include <stdint.h>
#include <string>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <unistd.h>
#include <vector>

/**
   Socket provides a clean interface around the Linux sockets API. Sockets can
   be used by both senders and receivers.

   To open a socket for sending, use the following API:

   Socket socket;
   socket.connect();
   int fd = socket.getFD(); // Actual socket file descriptor for send()

   To open sockets for receiving, use the following API:

   Socket listenSocket;
   listenSocket.listen();
   Socket* receiveSocket = socket.accept();
   int fd = receiveSocket->getFD() // Actual socket file descriptor for recv()
   delete receiveSocket;
 */
class Socket {
public:
  enum Mode {
    NONE,
    LISTENING,
    RECEIVING,
    SENDING
  };

  /// Constructor
  Socket();

  /// Destructor
  virtual ~Socket();

  /**
     Start listening for new connections.

     \param port the port to listen on

     \param backlogSize the maximum number of in-flight connection setups
   */
  void listen(const std::string& port, int backlogSize);

  /**
     Accept a new receiver socket.

     \param timeoutInMicros, the number of microseconds to wait before timing
     out and returning NULL, or 0 to induce a non-blocking accept behavior

     \param socketBufferSize the maximum buffer size of the newly accepted
     socket

     \return a new Socket object that can receive data, or NULL if the timeout
     expired
   */
  Socket* accept(uint64_t timeoutInMicros, uint64_t socketBufferSize);

  /**
     Connect to another peer in order to send data.

     \param address the address the other peer is listening on

     \param port the port the other peer is listening on

     \param socketBufferSize the maximum buffer size of the newly connected
     socket

     \param retryDelayInMicros the number of microseconds to wait before trying
     again if a connection fails

     \param maxRetries the number of retries to attempt before aborting
   */
  void connect(
    const std::string& address, const std::string& port,
    uint64_t socketBufferSize, uint64_t retryDelayInMicros,
    uint64_t maxRetries);

  /// Close an open socket.
  void close();

  /// \return the relevant address associated with the socket. For sending
  /// sockets, this is the address of the receiving peer. For receiving sockets,
  /// this is the address of the sending peer.
  const std::string& getAddress() const;

  /// \return the file descriptor associated with this socket
  int getFD() const;

  /**
     Set the peer ID for this connection. Usually application specific.

     \param peerID the ID of this peer
   */
  void setPeerID(uint64_t peerID);

  /// \return the ID of this peer
  uint64_t getPeerID() const;

  /**
     Set the flow ID for this connection. Used to distinguish multiple
     connections to the same peer.

     \param flowID the ID of this flow
   */
  void setFlowID(uint64_t flowID);

  /// \return the ID of this flow
  uint64_t getFlowID() const;

  /// \return true if the connection is closed
  bool closed() const;

private:
  static const int YES_REUSE;
  Mode mode;

  std::string address;
  int fd;
  uint64_t peerID;
  uint64_t flowID;

  struct pollfd acceptFDSet;
};

typedef std::vector<Socket*> SocketArray;

#endif // THEMIS_SOCKET_H
