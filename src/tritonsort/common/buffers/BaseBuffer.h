#ifndef TRITONSORT_GRAYSORT_V2_BASE_BUFFER_H
#define TRITONSORT_GRAYSORT_V2_BASE_BUFFER_H

#include <stdint.h>
#include <string.h>

#include "core/FixedSizeResource.h"
#include "core/TritonSortAssert.h"

class MemoryAllocatorInterface;
class WriteToken;

/**
   BaseBuffer contains data members and methods that can be universally useful
   for all buffers. It is deliberately designed to be minimal; any
   additional functionality should be implemented in subclasses.
 */
class BaseBuffer : public FixedSizeResource {
public:
  /// Constructor
  /**
     \param memoryAllocator a reference to the memory allocator that allocated
     memoryRegion

     \param callerID the caller ID that the constructor will use to allocate
     memory with memoryAllocator

     \param capacity the capacity of the buffer in bytes; if alignmentMultiple
     is non-zero, the actual allocated size of memoryRegion is expected to be
     at least alignmentMultiple bytes greater than capacity

     \param alignmentMultiple if the buffer's memory is to be aligned, the
     boundary on which it should be aligned
   */
  BaseBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
    uint64_t capacity, uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     \param memoryAllocator a reference to the memory allocator that allocated
     memoryRegion

     \param memoryRegion a pointer to the memory that will hold the buffer's
     data

     \param capacity the capacity of the buffer in bytes; if alignmentMultiple
     is non-zero, the actual allocated size of memoryRegion is expected to be
     at least alignmentMultiple bytes greater than capacity

     \param alignmentMultiple if the buffer's memory is to be aligned, the
     boundary on which it should be aligned
   */
  BaseBuffer(MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
             uint64_t capacity, uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     By default, buffers aren't I/O aligned. If you give them an alignment
     multiple, they'll align themselves and handle the destruction properly
     when they're freed just like IOAlignedBuffers used to.

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple if the buffer's memory is to be aligned, the
     boundary on which it should be aligned
  */
  BaseBuffer(uint64_t capacity, uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     \param memoryRegion a pointer to the memory that will hold the buffer's
     data

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple the buffer will access a portion of the memory
     region aligned to a multiple of this number for direct I/O reasons
   */
  BaseBuffer(uint8_t* memoryRegion, uint64_t capacity,
             uint64_t alignmentMultiple = 0);

  /// Destructor
  virtual ~BaseBuffer();

  /**
     Steals the underlying memory buffer of another BaseBuffer and sets its
     memory region to NULL. All relevant fields are copied as well. This method
     does delete this BaseBuffer's existing memory region before stealing the
     other. The primary use case of this method is to convert a buffer of one
     type to another type. For example, the KVPairFormatReader uses this to
     convert a ByteStreamBuffer to a KVPairBuffer without copying the underlying
     memory region.

     \param otherBuffer the buffer to steal from
   */
  void stealMemory(BaseBuffer& otherBuffer);

  /// Move the start of the buffer forward.
  /**
     Seeks forward some number of bytes.

     \param offset the number of bytes to move forward
   */
  void seekForward(uint64_t offset);

  /// Move the start of the buffer backward.
  /**
     Seeks backward some number of bytes.

     \param offset the number of bytes to move backward
   */
  void seekBackward(uint64_t offset);

  /// Append the given data to the end of the buffer, adjusting the length
  /// accordingly.
  /**
     \param data the data to append
     \param length the length of the data in bytes
   */
  inline virtual void append(const uint8_t* data, uint64_t length) {
    TRITONSORT_ASSERT(currentSize + length <= capacity,
           "Appending off the end of a buffer (wanted to append %llu bytes to "
           "a buffer with capacity of %llu bytes that currently has %llu bytes "
           "in it)", length, capacity, currentSize);
    memcpy(buffer + currentSize, data, length);

    currentSize += length;
  }

  /// Set up a "safe" append
  /**
     "Safe" appends allow you to append an amount of data directly to a buffer
   safely without knowing in advance how much data you're going to be
   appending and without mucking around with the buffer's internal state.

   This method declares how much data you will append to the buffer at most. If
   you can't append that much without overflowing, you'll fail an assertion.

   Do your appending, through a call to read(), or receive(), or whatever,
   using the pointer you received from BaseBuffer::setupAppend. If you
   decide you don't want to append, call abortAppend(), passing the char* you
   received from setupAppend().

   \param maximumAppendLength the promised maximum length of the data to be
   appended during this operation.

   \return a pointer to the portion of the buffer's internal memory where the
   append can be performed.
  */
  const uint8_t* setupAppend(uint64_t maximumAppendLength);

  /// Commit a "safe" append
  /**
     Use BaseBuffer::commitAppend to make the append you set up in
   BaseBuffer::setupAppend permanent. You must pass in the pointer that was
   given to you with setupAppend() and tell the buffer exactly how many bytes
   you appended so that it can update the buffer's internal state
   appropriately. If you pass a length that's longer than your promised
   maximum, or you pass the wrong pointer, you'll fail an assertion.

   \param ptr the data pointer that was returned by BaseBuffer::setupAppend

   \param actualAppendLength the amount of data that was written to the buffer
   beginning at the pointer given by ptr

   \sa BaseBuffer::setupAppend, BaseBuffer::abortAppend
   */
  virtual void commitAppend(const uint8_t* ptr, uint64_t actualAppendLength);

  /// Abort a "safe" append
  /**
     If you set up a safe append with BaseBuffer::setupAppend but decide that
     you don't want to do it later on, you can use BaseBuffer::abortAppend to
     abort the append. All safe appends must either be committed or aborted.

     \param ptr the data pointer that was returned by BaseBuffer::setupAppend

     \sa BaseBuffer::setupAppend, BaseBuffer::commitAppend
   */
  void abortAppend(const uint8_t* ptr);

  /// Clear the contents of a buffer
  inline virtual void clear() {
    currentSize = 0;
    pendingAppendPtr = NULL;
    pendingAppendLength = 0;
    token = NULL;
  }

  /// Get a pointer to the buffer's raw internal memory
  /**
     Used for copying, sending, etc. the buffer. "const uint8_t* const"
     indicates that you cannot change where the pointer points, nor can you
     change the data to which it points. This (hopefully) will prevent
     shenanigans.

     \return an immutable pointer to the buffer's internal memory
  */
  const uint8_t* const getRawBuffer() const;

  /// Get the buffer's capacity in bytes
  uint64_t getCapacity() const;

  /// Get the amount of data currently stored in the buffer in bytes
  uint64_t getCurrentSize() const;

  /// Set the current size of the buffer in bytes.
  /**
     If the new size is smaller than the current size, data at the end of the
     buffer will effectively be truncated. If the new size is larger than the
     current size, data will effectively be added at the end of the buffer, but
     that data's contents is undefined.

     \param size the amount of data now recognized to be in the buffer in bytes
   */
  void setCurrentSize(uint64_t size);

  /// Is the buffer empty?
  inline bool empty() const {
    return currentSize == 0;
  }

  /// Is the buffer full?
  inline bool full() const {
    return currentSize == capacity;
  }

  /// Simulate filling the buffer with data
  /**
     Used in synthetic workers to simulate filling the buffer without actually
     filling it. There are no guarantees as to the contents of the buffer.
   */
  void fillWithRandomData();

  /**
     Sets a WriteToken for this buffer. Used to synchronize chainer, coalescer,
     and writer.

     \param token the WriteToken for this buffer
   */
  inline void setToken(WriteToken* token) {
    this->token = token;
  }

  /**
     Gets the WriteToken associated with this buffer.

     \return the WriteToken for this buffer
   */
  inline WriteToken* getToken() {
    return token;
  }

protected:
  /// The buffer's capacity in bytes
  uint64_t capacity;

  /// The buffer's current size
  uint64_t currentSize;

  /// If a safe append is pending, this pointer is the location where the
  /// append will be done. If no safe append is pending, this is NULL.
  uint8_t* pendingAppendPtr;

  /// The maximum append size for a pending safe append, if any. If
  /// pendingAppendPtr is NULL, this variable's value is undefined.
  uint64_t pendingAppendLength;

private:
  /// Helper function that deletes the underlying memory buffer.
  void deleteBuffer();

  /// The maximum capacity of this buffer. While the actual capacity might
  /// change as the result of a seek, the maximum will never change unless
  /// stealMemory() is called
  uint64_t maximumCapacity;

  /// Determines the multiple to which the memory pointed to by buf is aligned
  uint64_t alignmentMultiple;

  /// If true, the buffer's internal memory will be deleted when the buffer is
  /// destructed
  bool deleteBufferOnDestruct;

  /// A pointer to the memory allocator that was used to allocate the buffer's
  /// internal memory; we have to keep this pointer around in certain
  /// circumstances so that we can garbage collect the memory allocated by the
  /// allocator properly
  MemoryAllocatorInterface* memoryAllocator;

  /// The raw underlying storage for this buffer. \warning Do not change this
  /// value. Unfortunately it cannot be made const because of the memory
  /// allocator's API that requires a context.
  uint8_t* bufferMemory;

  /// The raw buffer. This may differ from bufferMemory if a seek call
  /// has been issued. This is the buffer that actually gets returned by
  /// getRawBuffer() to hide the seek from the user.
  uint8_t* buffer;

  WriteToken* token;
};

#endif // TRITONSORT_GRAYSORT_V2_BASE_BUFFER_H
