#ifndef TRITONSORT_RAW_BUFFER_H
#define TRITONSORT_RAW_BUFFER_H

#include "TrackableResource.h"

/**
   A RawBuffer is a resource that encapsulates a raw uint8_t* byte buffer.

   RawBuffers are used to provide a ResourcePoolInterface that dynamically
   allocates byte buffers, rather than providing pre-allocated buffers.
 */
class RawBuffer : public TrackableResource {
public:
  /// Constructor
  /**
     \param _buffer the raw byte buffer to encapsulate

     \param _size the size in bytes of the byte buffer, for memory accounting

     \param cookie a tracking cookie pointer returned by a scheduler
   */
  RawBuffer(uint8_t* _buffer, uint64_t _size, const void* const cookie) :
    TrackableResource(cookie),
    buffer(_buffer),
    size(_size) {}

  /**
     \return the raw buffer associated with this RawBuffer resource
   */
  uint8_t* getBuffer() const {
    return buffer;
  }

  /// \sa Resourece::getCurrentSize
  uint64_t getCurrentSize() const {
    return size;
  }

private:
  uint8_t* const buffer;
  const uint64_t size;
};

#endif // TRITONSORT_RAW_BUFFER_H
