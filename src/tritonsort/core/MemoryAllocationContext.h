#ifndef THEMIS_MEMORY_ALLOCATION_CONTEXT_H
#define THEMIS_MEMORY_ALLOCATION_CONTEXT_H

#include <list>
#include <stdint.h>

class MemoryAllocationContext {
public:
  typedef std::list<uint64_t> MemorySizes;

  /// Constructor
  MemoryAllocationContext(
    uint64_t callerID, uint64_t size,
    bool failIfMemoryNotAvailableImmediately = false);

  void addSize(uint64_t size);

  const MemorySizes& getSizes() const;

  const uint64_t getCallerID() const;

  bool getFailIfMemoryNotAvailableImmediately() const;
private:
  const uint64_t callerID;
  const bool failIfMemoryNotAvailableImmediately;

  MemorySizes sizes;
};

#endif // THEMIS_MEMORY_ALLOCATION_CONTEXT_H
