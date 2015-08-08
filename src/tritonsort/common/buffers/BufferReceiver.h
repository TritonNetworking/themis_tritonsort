#ifndef TRITONSORT_BUFFER_RECEIVER_H
#define TRITONSORT_BUFFER_RECEIVER_H

#include <stdint.h>

#include "BaseBuffer.h"

/**
   BufferReceiver coordinates the non-blocking receipt of data from the network
   into buffers
 */
class BufferReceiver {
public:
  /// Constructor
  /**
     \param maxRecvSize the maximum size of a recv() syscall
   */
  BufferReceiver(uint64_t maxRecvSize);

  /**
     Read as much data from the given socket as possible (up to maxRecvSize) in
     a non-blocking fashion into the provided buffer, stopping if the buffer is
     full or the socket hangs up.

     \param buffer the buffer into which to receive

     \param socket the socket from which to receive

     \param[out] hungUp pointer to a boolean that is set to true if the socket
     hung up during this receive operation

     \return the number of bytes successfully read
   */
  uint64_t networkReceive(BaseBuffer* buffer, int socket, bool* hungUp);

private:
  const uint64_t maxRecvSize;
};

#endif // TRITONSORT_BUFFER_RECEIVER_H
