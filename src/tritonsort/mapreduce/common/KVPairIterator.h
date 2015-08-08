#ifndef THEMIS_KV_PAIR_ITERATOR_H
#define THEMIS_KV_PAIR_ITERATOR_H

#include "mapreduce/common/KeyValuePair.h"

/**
   KVPairIterator is designed to provide a general means by which callers can
   access records from some underlying source without caring about what
   that source is.
 */
class KVPairIterator {
public:
  /// Destructor
  virtual ~KVPairIterator() {}

  /// Retrieve the next record from the iterator
  /**
     \param[out] kvPair will be set to point to the contents of the next record

     \return true if the iterator had no records to return, false otherwise. If
     this method returns false, the values of kvPair's fields are undefined.
   */
  virtual bool next(KeyValuePair& kvPair) = 0;

  /// Reset the iterator
  virtual void reset() = 0;
};


#endif // THEMIS_KV_PAIR_ITERATOR_H
