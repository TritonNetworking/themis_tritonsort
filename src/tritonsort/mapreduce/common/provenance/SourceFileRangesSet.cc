#include <string.h>

#include "core/ByteOrder.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/provenance/SourceFileRangesSet.h"

SourceFileRangesSet* SourceFileRangesSet::demarshal(
  uint8_t* memory, uint64_t length) {

  uint64_t offset = 0;

  SourceFileRangesSet* rangesSet = new SourceFileRangesSet();

  while (offset < length) {
    uint64_t fileID = 0;
    uint64_t bytesDecoded = Base64::decode(
      memory + offset, 12, reinterpret_cast<uint8_t*>(&fileID));

    ABORT_IF(bytesDecoded != 8, "Expected to decode 8 bytes for file ID, but "
           "decoded %llu bytes", bytesDecoded);

    offset += 12;

    fileID = bigEndianToHost64(fileID);

    OffsetRanges& ranges = rangesSet->getOffsetRange(fileID);
    offset += ranges.demarshal(memory + offset);
  }

  ASSERT(offset == length, "Expected every byte to be demarshalled, but "
         "%llu/%llu bytes were demarshalled", offset, length);

  return rangesSet;
}

SourceFileRangesSet::SourceFileRangesSet() {

}

SourceFileRangesSet::~SourceFileRangesSet() {
  for (OffsetRangesMap::iterator iter = offsetRanges.begin();
       iter != offsetRanges.end(); iter++) {
    delete iter->second;
  }

  offsetRanges.clear();
}

void SourceFileRangesSet::add(
  uint64_t fileID, uint64_t startOffset, uint64_t endOffset) {

  ASSERT(startOffset < endOffset, "Expected that start offset (%llu) < end "
         "offset (%llu)", startOffset, endOffset);

  OffsetRanges& offsetRangesForFile = getOffsetRange(fileID);

  offsetRangesForFile.add(startOffset, endOffset);
}

void SourceFileRangesSet::merge(const SourceFileRangesSet& set) {
  const OffsetRangesMap& mergingOffsetRanges = set.offsetRanges;

  for (OffsetRangesMap::const_iterator mapIter = mergingOffsetRanges.begin();
       mapIter != mergingOffsetRanges.end(); mapIter++) {
    uint64_t fileID = mapIter->first;
    const OffsetRanges* fileRanges = mapIter->second;

    getOffsetRange(fileID).merge(*fileRanges);
  }
}

void SourceFileRangesSet::marshal(uint8_t*& memory, uint64_t& length) const {
  length = 0;

  // We'll be marshaling, for each file: the Base64-encoded file ID (12 bytes),
  // then the Base64-encoded OffsetRanges for that file ID

  // Iterate through the structure once to figure out how long to make the
  // memory region

  for (OffsetRangesMap::const_iterator mapIter = offsetRanges.begin();
       mapIter != offsetRanges.end(); mapIter++) {
    length += 12 + (mapIter->second)->marshalledSize();
  }

  memory = new (themis::memcheck) uint8_t[length];

  uint64_t offset = 0;

  for (OffsetRangesMap::const_iterator mapIter = offsetRanges.begin();
       mapIter != offsetRanges.end(); mapIter++) {
    uint64_t fileID = mapIter->first;
    const OffsetRanges& ranges = *(mapIter->second);

    fileID = hostToBigEndian64(fileID);

    Base64::encode(
      reinterpret_cast<uint8_t*>(&fileID), sizeof(uint64_t), memory + offset);
    offset += 12;

    ranges.marshal(memory + offset);
    offset += ranges.marshalledSize();
  }
}

bool SourceFileRangesSet::equals(SourceFileRangesSet& other) const {
  if (offsetRanges.size() != other.offsetRanges.size()) {
    return false;
  }

  for (iterator iter = offsetRanges.begin(); iter != offsetRanges.end();
       iter++) {
    uint64_t fileID = iter->first;
    const OffsetRanges& ranges = *(iter->second);

    iterator otherIter = other.offsetRanges.find(fileID);

    if (otherIter == other.offsetRanges.end()) {
      return false;
    }

    const OffsetRanges& otherRanges = *(otherIter->second);

    if (!ranges.equals(otherRanges)) {
      return false;
    }
  }

  return true;
}

const SourceFileRangesSet::iterator
SourceFileRangesSet::begin() const {
  return offsetRanges.begin();
}


const SourceFileRangesSet::iterator
SourceFileRangesSet::end() const {
  return offsetRanges.end();
}
