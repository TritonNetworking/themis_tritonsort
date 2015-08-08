#ifndef TRITONSORT_BUFFER_LIST_CONTAINER_H
#define TRITONSORT_BUFFER_LIST_CONTAINER_H

#include "buffers/BufferList.h"
#include "common/WriteToken.h"
#include "core/Resource.h"

/**
   The BufferListContainer resource encapsulates a BufferList and a WriteToken
   in a single work unit. It's used to pass a buffer list and the token
   indicating the disk to which the coalesced version of the buffer list
   should be directed.
 */
template <typename T> class BufferListContainer : public Resource {
public:
  /// Constructor
  BufferListContainer(uint64_t _jobID, WriteToken* _token)
    : jobID(_jobID), token(_token) {
  }

  /// Destructor
  /**
     \warning the caller is responsible for returning the container's token to
     its appropriate pool
   */
  virtual ~BufferListContainer() {
  }

  uint64_t getCurrentSize() const {
    return list.getTotalDataSize();
  }

  /// Get the token from inside the container
  /**
     \return a pointer to the write token contained in this container, or NULL
     if no token has been set
   */
  WriteToken* getToken() const {
    return token;
  }

  uint64_t getJobID() const {
    return jobID;
  }

  /// Get the buffer list from the container
  /**
     \return a reference to the container's buffer list
   */
  inline BufferList<T>& getList() {
    return list;
  }

  /// Get the disk ID for which the container is destined
  /**
     The write token for this container must be non-NULL before calling this
     method.

     \return the disk ID for which the container is destined, retrieved from
     the container's write token
   */
  inline uint64_t getDiskID() const {
    ASSERT(token != NULL, "Can't call getDiskID without setting token first");
    return token->getDiskID();
  }

private:
  const uint64_t jobID;
  WriteToken* token;
  BufferList<T> list;
};


#endif // TRITONSORT_BUFFER_LIST_CONTAINER_H
