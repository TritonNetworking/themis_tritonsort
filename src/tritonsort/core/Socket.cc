#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "core/Socket.h"
#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"

const int Socket::YES_REUSE = 1;

Socket::Socket()
  : mode(NONE),
    address(""),
    fd(-1) {
}

Socket::~Socket() {
  ABORT_IF(mode != NONE, "Socket initialized at destruction time.");
  ABORT_IF(fd != -1, "File descriptor %d open at destruction time", fd);
}

void Socket::listen(const std::string& port, int backlogSize) {
  ABORT_IF(mode != NONE, "Cannot call listen() on an open socket.");
  mode = LISTENING;

  // Create listening socket and set socket options.
  fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

  if (fd < 0) {
    ABORT("socket() failed with status %d: %s", errno, strerror(errno));
  }

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                 (char *)&YES_REUSE, sizeof(YES_REUSE))) {
    ABORT("setsockopt(REUSEADDR) failed with status %d: %s",
          errno, strerror(errno));
  }

  // Bind to the appropriate port.
  struct sockaddr_in listenAddress;
  memset(&listenAddress, 0, sizeof(listenAddress));
  listenAddress.sin_family = AF_INET;
  listenAddress.sin_addr.s_addr = htonl(INADDR_ANY);
  listenAddress.sin_port = htons(atoi(port.c_str()));

  if (bind(fd, (struct sockaddr *) &listenAddress, sizeof(listenAddress)) < 0) {
    ABORT("bind(%s) failed with status %d: %s", port.c_str(),
          errno, strerror(errno));
  }

  // Listen for new connections with the appropriate backlog size.
  if (::listen(fd, backlogSize) < 0) {
    ABORT("listen() failed with status %d: %s", errno, strerror(errno));
  }

  // Set up the FD set for accept calls.
  FD_ZERO(&acceptFDSet);
  FD_SET(fd, &acceptFDSet);
}

Socket* Socket::accept(uint64_t timeoutInMicros, uint64_t socketBufferSize) {
  ABORT_IF(mode != LISTENING,
           "Cannot call accept() until after calling listen()");

  if (timeoutInMicros > 0) {
    // Issue a select() on the listening socket.
    fd_set readFDSet;
    int fdMax = fd;
    int status;
    struct timeval timeout;

    readFDSet = acceptFDSet;

    timeout.tv_sec = timeoutInMicros / 1000000;
    timeout.tv_usec = timeoutInMicros % 1000000;

    status = select(fdMax + 1, &readFDSet, NULL, NULL, &timeout);
    ABORT_IF(status == -1,
             "select() failed with status %d: %s", errno, strerror(errno));

    // If the select() call timed out, return NULL.
    if (!FD_ISSET(fd, &readFDSet)) {
      return NULL;
    }
  }

  // Issue a blocking accept() call on the listening socket.
  struct sockaddr_in clientAddress;
  unsigned int addressLength = sizeof(clientAddress);

  int clientSocket = -1;

  if ((clientSocket = ::accept(fd, (struct sockaddr *) & clientAddress,
                               &addressLength)) < 0) {
    ABORT("accept() failed with status %d: %s", errno, strerror(errno));
  }

  std::string clientIPAddress = inet_ntoa(clientAddress.sin_addr);

  // Set the TCP receive buffer size
  if (socketBufferSize > 0 && setsockopt(
        clientSocket, SOL_SOCKET, SO_RCVBUF, (char *)&socketBufferSize,
        sizeof(socketBufferSize))) {
    ABORT("setsocktopt(SO_RCVBUF) with size %d failed with status %d: %s",
          socketBufferSize, errno, strerror(errno));
  }

  // Create a new Socket object for this client connection.
  Socket* socket = new Socket();
  socket->address.assign(clientIPAddress);
  socket->fd = clientSocket;
  socket->mode = RECEIVING;

  return socket;
}

void Socket::connect(
  const std::string& serverAddress, const std::string& serverPort,
  uint64_t socketBufferSize, uint64_t retryDelayInMicros, uint64_t maxRetries) {
  ABORT_IF(mode != NONE, "Cannot call connect() on an open socket");
  mode = SENDING;

  // Get network address information.
  struct addrinfo hints, *res;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  int status = getaddrinfo(
    serverAddress.c_str(), serverPort.c_str(), &hints, &res);
  ABORT_IF(status == -1, "getaddrinfo() failed with error code %d: %s",
           errno, strerror(errno));

  // Create a socket for this connection and set socket options.
  fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  ABORT_IF(fd == -1, "socket() failed with error code %d: %s",
           errno, strerror(errno));

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                 (char *)&YES_REUSE, sizeof(YES_REUSE))) {
    ABORT("setsockopt(REUSEADDR) failed with status %d: %s",
          errno, strerror(errno));
  }

  // Set the TCP send buffer size
  if (socketBufferSize > 0 && setsockopt(
        fd, SOL_SOCKET, SO_SNDBUF, (char *)&socketBufferSize,
        sizeof(socketBufferSize))) {
    ABORT("setsocktopt(SO_SNDBUF) with size %d failed with status %d: %s",
          socketBufferSize, errno, strerror(errno));
  }

  // Try to connect to the address some number of times before giving up.
  uint32_t retries = 0;
  status = -1;

  do {
    StatusPrinter::add("Connecting to %s:%s on socket %d",
                       serverAddress.c_str(), serverPort.c_str(), fd);

    status = ::connect(fd, res->ai_addr, res->ai_addrlen);

    if (status == -1) {
      StatusPrinter::add("connect() to %s:%s failed with error %d: %s",
                         serverAddress.c_str(), serverPort.c_str(), errno,
                         strerror(errno));
      usleep(retryDelayInMicros);
      retries++;
    }
  } while (status == -1 && retries < maxRetries);

  freeaddrinfo(res);

  ABORT_IF(retries == maxRetries && status == -1,
           "Retried opening connection to %s:%s %d times, and failed each "
           "time. Giving up.", serverAddress.c_str(), serverPort.c_str(),
           maxRetries);

  address.assign(serverAddress);
}

void Socket::close() {
  ABORT_IF(mode == NONE, "Cannot call close() on an uninitialized socket.");
  ABORT_IF(fd < 0, "Cannot call close() on a closed socket.");
  int status = ::close(fd);
  ABORT_IF(status == -1,
           "close() failed with error %d: %s", errno, strerror(errno));
  fd = -1;
  mode = NONE;
}

const std::string& Socket::getAddress() const {
  return address;
}

int Socket::getFD() const {
  return fd;
}

void Socket::setPeerID(uint64_t _peerID) {
  peerID = _peerID;
}

uint64_t Socket::getPeerID() const {
  return peerID;
}

void Socket::setFlowID(uint64_t _flowID) {
  flowID = _flowID;
}

uint64_t Socket::getFlowID() const {
  return flowID;
}

bool Socket::closed() const {
  return (fd == -1);
}
