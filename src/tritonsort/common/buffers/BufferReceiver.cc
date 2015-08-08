#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "BufferReceiver.h"

BufferReceiver::BufferReceiver(uint64_t _maxRecvSize)
  : maxRecvSize(_maxRecvSize) {
}

uint64_t BufferReceiver::networkReceive(BaseBuffer* buffer, int socket,
                                        bool* hungUp) {
  if (hungUp != NULL) {
    *hungUp = false;
  }

  if (buffer->full()) {
    return 0;
  }

  uint64_t bufferCapacity = buffer->getCapacity();
  uint64_t currentBufferSize = buffer->getCurrentSize();

  uint64_t amtToBeRead = std::min(bufferCapacity - currentBufferSize,
                                  maxRecvSize);

  const uint8_t* appendBuf = buffer->setupAppend(amtToBeRead);

  ssize_t ret = recv(socket, (uint8_t*) appendBuf, amtToBeRead, MSG_DONTWAIT);

  if (ret < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      /* Not really an error, its just that this peer
       * didn't have anything to send
       */
      buffer->abortAppend(appendBuf);
      return 0;
    } else {
      ABORT("Socket %d recv() returned errno %d (%s)",
            socket, errno, strerror(errno));
    }
  } else if (ret == 0) {
    /* The peer performed an orderly shutdown */
    buffer->abortAppend(appendBuf);
    if (hungUp) {
      *hungUp = true;
    }
    return 0;
  }

  uint64_t amtActuallyRead = (uint64_t) ret;
  ASSERT(amtActuallyRead <= amtToBeRead, "read more than we wanted");

  buffer->commitAppend(appendBuf, amtActuallyRead);

  return amtActuallyRead;
}
