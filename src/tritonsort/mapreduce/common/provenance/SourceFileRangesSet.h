#ifndef THEMIS_SOURCE_FILE_RANGES_SET_H
#define THEMIS_SOURCE_FILE_RANGES_SET_H

#include <deque>
#include <map>
#include <stdint.h>

#include "core/constants.h"
#include "mapreduce/common/provenance/OffsetRanges.h"

/**
   This class is used to store information about which portions of which files
   the records in a buffer came from.
 */
class SourceFileRangesSet {
public:
  static SourceFileRangesSet* demarshal(uint8_t* memory, uint64_t length);

  /// Constructor
  SourceFileRangesSet();

  /// Destructor
  virtual ~SourceFileRangesSet();

  /// Add a range from a particular file to this set
  /**
     This method performs a trivial form of range merging that assumes that the
     offsets it receives for any given file are strictly increasing; that is,
     that startOffset will be >= the value of endOffset for this file in any
     previous call to SourceFileRangesSet::add.

     \param fileID the GUID of the file over which the range applies

     \param startOffset the offset in the file where the range begins

     \param startOffset the offset in the file where the range ends
   */
  void add(uint64_t fileID, uint64_t startOffset, uint64_t endOffset);

  /// Merge the ranges in another SourceFileRangesSet into this one
  /**
     \param set the SourceFileRangesSet to merge
   */
  void merge(const SourceFileRangesSet& set);

  /// Marshal this SourceFileRangesSet into memory
  /**
     \param[out] memory a memory region that this method will create containing
     the marshaled object. Caller is responsible for destructing this region.

     \param[out] length the length of the region in bytes
   */
  void marshal(uint8_t*& memory, uint64_t& length) const;

  bool equals(SourceFileRangesSet& other) const;

  typedef std::map<uint64_t, OffsetRanges*> OffsetRangesMap;
  typedef OffsetRangesMap::const_iterator iterator;

  const SourceFileRangesSet::iterator begin() const;
  const SourceFileRangesSet::iterator end() const;

private:
  inline OffsetRanges& getOffsetRange(uint64_t fileID) {
    OffsetRangesMap::iterator iter = offsetRanges.find(fileID);

    if (iter == offsetRanges.end()) {
      iter = offsetRanges.insert(std::make_pair(fileID, new OffsetRanges()))
        .first;
    }

    return *(iter->second);
  }

  OffsetRangesMap offsetRanges;
};


#endif // THEMIS_SOURCE_FILE_RANGES_SET_H
