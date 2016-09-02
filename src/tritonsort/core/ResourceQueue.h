#ifndef TRITONSORT_RESOURCE_QUEUE_H
#define TRITONSORT_RESOURCE_QUEUE_H

#include <list>
#include <stdint.h>
#include <string.h>

#include "core/Resource.h"
#include "core/TritonSortAssert.h"

/**
   A fixed-size FIFO queue that supports efficient bulk pushes
 */
class ResourceQueue {
public:
  /// Constructor
  /**
     \param cap the queue's capacity
   */
  ResourceQueue(uint64_t cap) {
    curSize = 0;
    frontOffset = 0;
    backOffset = 0;
    capacity = cap;
    resources = new Resource*[capacity];
    memset(resources, 0, sizeof(Resource*) * capacity);
  }

  /// Destructor
  virtual ~ResourceQueue() {
    if (resources != NULL) {
      delete[] resources;
    }
  }

  /// Is the queue empty?
  /**
     \return if the queue is empty or not
   */
  inline bool empty() const {
    return curSize == 0;
  }

  /// Is the queue full?
  /**
     \return if the queue is full or not
   */
  inline bool full() const {
    return curSize == capacity;
  }

  /// Get the size of the queue
  /**
     \return the size of the queue
   */
  inline uint64_t size() const {
    return curSize;
  }

  /// Push an element onto the back of the queue
  /**
     \param x the element to push
   */
  inline void push(Resource* x) {
    TRITONSORT_ASSERT(curSize < capacity, "Tried to push into a full pool");
    resources[backOffset] = x;
    backOffset = (backOffset + 1) % capacity;
    curSize++;
  }

  /// Pop an element from the front of the queue
  /**
     \warning Must check if the pool is empty with ResourceQueue::empty before
     popping.
   */
  inline void pop() {
    TRITONSORT_ASSERT(curSize > 0, "Tried to pop from an empty pool");
    resources[frontOffset] = NULL;
    frontOffset = (frontOffset + 1) % capacity;
    curSize--;

    if (curSize == 0) {
      frontOffset = 0;
      backOffset = 0;
    }
  }

  /// Get the element at the front of the queue
  /**
     \warning Must check if the queue is empty with ResourceQueue::empty before
     calling.

     \return the element at the front of the queue
   */
  inline Resource* front() {
    return resources[frontOffset];
  }

  /// Get the capacity of the queue
  /**
     \return the capacity of the queue
   */
  inline uint64_t getCapacity() const {
    return capacity;
  }

  /// Push a number of elements from the front of another queue onto the back
  /// of this queue efficiently
  /**
     This method assumes (and, if assertions are enabled, asserts) that there
     is enough space in the queue to accomodate the elements to be pushed.

     \param otherQueue the queue from which to retrieve elements

     \param numElements the number of elements to retrieve
   */
  inline void stealFrom(ResourceQueue& otherQueue, uint64_t numElements) {
    if (numElements == 0) {
      return;
    }

    ResourceQueue& other = otherQueue;

    uint64_t amtLeftToCopy = numElements;

    TRITONSORT_ASSERT(numElements <= other.curSize,
      "Trying to copy more elements than the source pool has");
    TRITONSORT_ASSERT(amtLeftToCopy + curSize <= capacity, "Not enough space left in "
           "dest. pool (%llu) to copy %llu elements", capacity - curSize,
           amtLeftToCopy);

    while (amtLeftToCopy > 0) {
      uint64_t destCellsFree = 0;

      if (frontOffset > backOffset) {
        destCellsFree = frontOffset - backOffset;
      } else if (frontOffset == backOffset) {
        if (curSize == 0) {
          destCellsFree = capacity;
        } else {
          ABORT("Previous assertions should have caught this");
        }
      } else {
        destCellsFree = capacity - backOffset;
      }

      destCellsFree = std::min<uint64_t>(destCellsFree, amtLeftToCopy);

      uint64_t srcCellsBeforeEnd = 0;

      if (other.frontOffset >= other.backOffset) {
        TRITONSORT_ASSERT(other.curSize > 0,
               "Previous assertions should have caught this");
        srcCellsBeforeEnd = other.capacity - other.frontOffset;
      } else {
        srcCellsBeforeEnd = other.backOffset - other.frontOffset;
      }

      srcCellsBeforeEnd = std::min<uint64_t>(srcCellsBeforeEnd, amtLeftToCopy);

      uint64_t cellsToCopy = std::min<uint64_t>(srcCellsBeforeEnd,
                                                destCellsFree);

      TRITONSORT_ASSERT(cellsToCopy > 0, "Somehow decided that you need to copy zero "
             "cells");

      memcpy(resources + backOffset, other.resources + other.frontOffset,
             cellsToCopy * sizeof(Resource*));
      memset(other.resources + other.frontOffset, 0,
             cellsToCopy * sizeof(Resource*));
      other.frontOffset = (other.frontOffset + cellsToCopy) % other.capacity;
      backOffset = (backOffset + cellsToCopy) % capacity;
      amtLeftToCopy -= cellsToCopy;
      other.curSize -= cellsToCopy;
      curSize += cellsToCopy;
    }

    if (other.curSize == 0) {
      other.frontOffset = 0;
      other.backOffset = 0;
    }
  }
private:
  Resource** resources;
  // Offset in the resources array that points to the front of the queue
  uint64_t frontOffset;
  // Offset in the resources array that points to the element after the back of
  // the queue
  uint64_t backOffset;
  uint64_t curSize;
  uint64_t capacity;
};

#endif // TRITONSORT_RESOURCE_QUEUE_H
