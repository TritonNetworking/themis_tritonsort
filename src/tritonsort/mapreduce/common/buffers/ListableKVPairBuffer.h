#ifndef MAPRED_LISTABLE_KV_PAIR_BUFFER_H
#define MAPRED_LISTABLE_KV_PAIR_BUFFER_H

#include "KVPairBuffer.h"

/**
   ListableKVPairBuffers are used to form large chains of small buffers to be
   coalesced together before writing. This enables phase 1 to write in large
   chunks to avoid seeking.

   This class effectively implements a doubly-linked list of KVPairBuffers.
 */
class ListableKVPairBuffer : public KVPairBuffer {
public:
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
  ListableKVPairBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple = 0);

  /**
     Get the previous element in the list.

     \return the previous ListableKVPairBuffer
   */
  inline ListableKVPairBuffer* getPrev() {
    return prev;
  }

  /**
     Set the previous element in the list

     \param prev the ListableKVPairBuffer that comes before this one
   */
  inline void setPrev(ListableKVPairBuffer* prev) {
    this->prev = prev;
  }

  /**
     Get the next element in the list.

     \return the next ListableKVPairBuffer
   */
  inline ListableKVPairBuffer* getNext() {
    return next;
  }

  /**
     Set the next element in the list

     \param next the ListableKVPairBuffer that comes after this one
   */
  inline void setNext(ListableKVPairBuffer* next) {
    this->next = next;
  }

private:
  ListableKVPairBuffer* prev;
  ListableKVPairBuffer* next;

  uint64_t logicalDiskID;
};

#endif // MAPRED_LISTABLE_KV_PAIR_BUFFER_H
