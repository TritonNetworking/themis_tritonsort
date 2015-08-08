#ifndef TRITONSORT_BUFFER_LIST_H
#define TRITONSORT_BUFFER_LIST_H

#include <limits>

#include "core/TritonSortAssert.h"

/**
   A BufferList is used to chain together buffers into a doubly-linked list,
   where each buffer is destined for the same logical and physical disk IDs.

   The buffers that a BufferList accepts are duck-typed to require the
   following methods:

   T* getPrev() / void setPrev(T*) - get/set the pointer to the previous
   element in the list

   T* getNext() / void setNext(T*) - get/set the pointer to the next element in
   the list

   Buffers can only be associated with one BufferList at a time.
 */
template <typename T> class BufferList  {
public:
  /// Constructor
  /**
     Sets the logical and physical disk IDs for the buffer list to some
     unreasonable number (max. value of uint64_t) and clears it
   */
  BufferList() {
    logicalDiskID = std::numeric_limits<uint64_t>::max();
    physicalDiskID = std::numeric_limits<uint64_t>::max();
    clear();
  }

  /// Constructor
  /**
     \param _logicalDiskID the logical disk ID for the buffers in this list
     \param _physicalDiskID the physical disk ID for the buffers in this list
   */
  BufferList(uint64_t _logicalDiskID, uint64_t _physicalDiskID)
    : logicalDiskID(_logicalDiskID), physicalDiskID(_physicalDiskID) {
    clear();
  }

  /// Get the number of buffers in the list
  /**
     \return the number of buffers in the list
   */
  uint64_t getSize() const {
    return size;
  }

  /// Get the total amount of data contained in buffers in this list
  /**
     \return the total amount of data in this list's buffers
   */
  uint64_t getTotalDataSize() const {
    return totalDataSize;
  }

  /**
     Move many buffers from one list to another bounded by some maximum number
     of bytes to transfer.

     \param destinationList the target list, from which elements will be moved

     \param maxBytesToMove the maximum number of buffers to move in terms of
     total bytes

     \return the number of bytes moved from the target list to this list
  */
  uint64_t bulkMoveBuffersTo(
    BufferList<T>& destinationList, uint64_t maxBytesToMove) {

    uint64_t bytesMoved = 0;

    if (maxBytesToMove >= totalDataSize) {
      // If the requested size is large enough that we can simply move the
      // entire list, just transfer this list's contents to the other list and
      // avoid the overhead of a bunch of appends

      bytesMoved = size;

      destinationList.size = size;
      destinationList.totalDataSize = totalDataSize;
      destinationList.head = head;
      destinationList.tail = tail;

      size = 0;
      totalDataSize = 0;
      head = NULL;
      tail = NULL;
    } else {
      while (size > 0 && bytesMoved < maxBytesToMove) {
        // Check to see if the head is small enough to append.
        ASSERT(head != NULL, "About to append NULL to a list.");
        uint64_t headSize = head->getCurrentSize();
        // The only buffer that is allowed to exceed the maximum list size is
        // the first buffer. This allows large records to pass through the
        // Chainer as a length-1 chain.
        if (bytesMoved + headSize > maxBytesToMove && bytesMoved > 0) {
          // Appending the head would exceed the maximum bulk move size.
          break;
        }

        // Move the head to this list.
        destinationList.append(removeHead());
        bytesMoved += headSize;
      }
    }

    return bytesMoved;
  }

  /// Get the head of the list but do not remove it.
  /**
     \return a pointer to the buffer at the head of the list, or NULL if the
     list is empty
   */
  T* getHead() const {
    return head;
  }

  /// Remove the head of the list and return it
  /**
     \return a pointer to the buffer at the head of the list, or NULL if the
     list is empty
   */
  T* removeHead() {
    T* currentHead = head;

    if (head != NULL) {
      totalDataSize -= currentHead->getCurrentSize();
      head = head->getNext();
      currentHead->setNext(NULL);
      currentHead->setPrev(NULL);

      if (head != NULL) {
        head->setPrev(NULL);
      }

      size--;
    }

    if (size == 0) {
      tail = NULL;
    }

    return currentHead;
  }

  /// Is this buffer list empty?
  bool empty() const {
    return size == 0;
  }

  /// Append a buffer to the end of the list
  /**
     \param buf a pointer to the buffer to append to the list
   */
  void append(T* buf) {
    ASSERT(buf->getPrev() == NULL && buf->getNext() == NULL, "Buffer can't be "
           "appended because it is already part of a list");

    if (head == NULL) {
      head = buf;
    }

    if (tail != NULL) {
      tail->setNext(buf);
      buf->setPrev(tail);
    }

    tail = buf;
    size++;
    totalDataSize += buf->getCurrentSize();
  }

  /// Clear the buffer list
  /**
     At the conclusion of this method, the buffer list will be empty.

     \warning This does not clear the prev and next pointers in any buffers
     that made up the list previously; don't rely on this to clear your
     buffers' prev and next pointers.
   */
  void clear() {
    size = 0;
    totalDataSize = 0;
    head = NULL;
    tail = NULL;
  }

  /// Get the logical disk ID for this buffer list
  uint64_t getLogicalDiskID() const {
    return logicalDiskID;
  }

  /// Set the logical disk ID for this buffer list
  void setLogicalDiskID(uint64_t logicalDiskID) {
    this->logicalDiskID = logicalDiskID;
  }

  /// Get the physical disk ID for this buffer list
  uint64_t getPhysicalDiskID() const {
    return physicalDiskID;
  }

  /// Set the physical disk ID for this buffer list
  void setPhysicalDiskID(uint64_t physicalDiskID) {
    this->physicalDiskID = physicalDiskID;
  }

private:
  uint64_t logicalDiskID, physicalDiskID;
  T* head;
  T* tail;

  uint64_t totalDataSize;
  uint64_t size;
};

#endif // TRITONSORT_BUFFER_LIST_H
