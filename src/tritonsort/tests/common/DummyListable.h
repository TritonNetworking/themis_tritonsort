#ifndef TRITONSORT_DUMMY_LISTABLE_H
#define TRITONSORT_DUMMY_LISTABLE_H

#include <stdint.h>
#include <stdlib.h>

class DummyListable {
public:
  DummyListable()
    : size(0),
      next(NULL),
      prev(NULL) {}

  DummyListable* getNext() const {
    return next;
  }

  void setNext(DummyListable* next) {
    this->next = next;
  }

  DummyListable* getPrev() const {
    return prev;
  }

  void setPrev(DummyListable* prev) {
    this->prev = prev;
  }

  uint64_t getCurrentSize() const {
    return size;
  }

  void setSize(uint64_t size) {
    this->size = size;
  }

  uint64_t getSize() const {
    return size;
  }

private:
  uint64_t size;
  DummyListable* next;
  DummyListable* prev;
};

#endif // TRITONSORT_DUMMY_LISTABLE_H
