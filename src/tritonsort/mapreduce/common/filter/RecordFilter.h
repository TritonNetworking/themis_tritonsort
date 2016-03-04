#ifndef THEMIS_RECORD_FILTER_H
#define THEMIS_RECORD_FILTER_H

#include <stdint.h>

#include "core/constants.h"

class PartitionBoundaries;

/**
   RecordFilter is used to determine whether a key falls into a set of
   partition ranges, so that it can be emitted or not as appropriate during
   recovery
 */
class RecordFilter {
public:
  /// Constructor
  RecordFilter();

  /// Destructor
  virtual ~RecordFilter();

  /// Add a partition boundary to the set of boundaries passed by the filter
  /**
     \param boundariesToAdd a PartitionBoundaries describing the key range
     allowed by the filter. This object will garbage collect the
     PartitionBoundaries object when it destructs
   */
  void addPartitionBoundaries(PartitionBoundaries& boundariesToAdd);

  /// Is the given key contained in one of the filter's key ranges?
  /**
     \param key the key to check
     \param keyLength the length of the key

     \return true if this key is contained in one or more of the filter's key
     ranges, false otherwise
   */
  bool pass(const uint8_t* key, uint32_t keyLength) const;

private:
  typedef std::vector<PartitionBoundaries*> PartitionBoundaryVector;
  PartitionBoundaryVector boundaries;
};


#endif // THEMIS_RECORD_FILTER_H
