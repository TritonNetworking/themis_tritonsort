#ifndef TRITONSORT_BYTE_STREAM_BUFFER_H
#define TRITONSORT_BYTE_STREAM_BUFFER_H

#include "BaseBuffer.h"

/**
   ByteStreamBuffer is a simple extension of BaseBuffer that contains a stream
   identifier. A byte stream buffer is intended on carrying unformatted bytes.
   These buffers can be safely "streamed" from IO devices or from the network.
   A converter may be required to handle the byte stream according to the given
   application.
 */
class ByteStreamBuffer : public BaseBuffer {
public:
  /// Constructor
  /**
     \sa BaseBuffer::BaseBuffer
   */
  ByteStreamBuffer(MemoryAllocatorInterface& memoryAllocator,
                   uint64_t callerID, uint64_t capacity,
                   uint64_t alignmentMultiple);

  /// Constructor
  /**
     \param memoryAllocator the memory allocator that was used to allocate
     memoryRegion

     \param memoryRegion a pointer to the memory that will hold the buffer's
     data

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple if the buffer's memory is to be aligned, the
     boundary on which it should be aligned

   */
  ByteStreamBuffer(MemoryAllocatorInterface& memoryAllocator,
                   uint8_t* memoryRegion, uint64_t capacity,
                   uint64_t alignmentMultiple = 0);

  /// Destructor
  virtual ~ByteStreamBuffer() {}

  /// Get the stream ID for the buffer. This could be a node, or a disk, or
  /// anything distinguishing one stream from another.
  /**
     \return the streamID of the buffer
   */
  uint64_t getStreamID();

  /// Set the stream ID for the buffer.
  /**
     \param streamID the stream ID to set
   */
  void setStreamID(uint64_t streamID);
private:
  uint64_t streamID;
};

#endif // TRITONSORT_BYTE_STREAM_BUFFER_H
