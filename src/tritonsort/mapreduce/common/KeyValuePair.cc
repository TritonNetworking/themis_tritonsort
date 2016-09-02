#include <stdlib.h>
#include <string.h>

#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"

KeyValuePair::KeyValuePair()
  : readHeader(true),
    writeHeader(true),
    fixedKeyLength(0),
    fixedValueLength(0),
    tupleStart(NULL),
    _keyLength(0),
    _key(NULL),
    _valueLength(0),
    _value(NULL) {
    }

KeyValuePair::~KeyValuePair() {
}

void KeyValuePair::setWriteWithoutHeader(bool writeWithoutHeader) {
  writeHeader = !writeWithoutHeader;
}

bool KeyValuePair::getWriteWithoutHeader() {
  return !writeHeader;
}

void KeyValuePair::readWithoutHeader(
  uint32_t _fixedKeyLength, uint32_t _fixedValueLength) {
  fixedKeyLength = _fixedKeyLength;
  fixedValueLength = _fixedValueLength;
  readHeader = false;
}

uint64_t KeyValuePair::serializeHeader(uint8_t* destination) const {
  // If we're not using a header just no-op.
  if (writeHeader) {
    KeyValuePairHeader* header =
      reinterpret_cast<KeyValuePairHeader*>(destination);

    header->keyLength = _keyLength;
    header->valueLength = _valueLength;

    // Serialized HEADER_SIZE bytes.
    return HEADER_SIZE;
  } else {
    // Didn't serialize any bytes.
    return 0;
  }
}

void KeyValuePair::serialize(uint8_t* destination) const {
  TRITONSORT_ASSERT(destination != NULL,
         "Cannot serialize() KeyValuePair to a NULL destination");

  if (tupleStart != NULL &&
      ((readHeader && writeHeader) || (!readHeader && !writeHeader))) {
    // The tuple exists in contiguous memory and the read and write formats are
    // the same, so we can fast-path by copying the entire tuple start to finish
    memcpy(destination, tupleStart, getWriteSize());
  } else if (tupleStart != NULL && readHeader && !writeHeader) {
    // The tuple exists in contiguous memory and we have a header, but we're not
    // writing it.
    memcpy(destination, _key, getWriteSize());
  } else if (tupleStart != NULL && !readHeader && writeHeader) {
    // The tuple exists in contiguous memory but we don't have a header and we
    // need to write one.
    uint64_t offset = serializeHeader(destination);

    // Now write the key and value together.
    memcpy(destination + offset, _key, getReadSize());
  } else {
    // The tuple doesn't exist in contiguous memory, so the header, key, and
    // value need to be written separately.
    uint64_t offset = serializeHeader(destination);
    uint8_t* destPtr = destination + offset;

    if (_keyLength > 0) {
      destPtr = static_cast<uint8_t*>(mempcpy(destPtr, _key, _keyLength));
    }

    if (_valueLength > 0) {
      destPtr = static_cast<uint8_t*>(mempcpy(destPtr, _value, _valueLength));
    }
  }
}
void KeyValuePair::partialSerialize(
  uint8_t* destination, uint64_t offset, uint64_t length) const {
  TRITONSORT_ASSERT(destination != NULL,
         "Cannot partiallSerialize() KeyValuePair to a NULL destination");
  TRITONSORT_ASSERT(offset < getWriteSize(),
         "Cannot partialSerialize() at offset %llu beyond tuple length %llu.",
         offset, getWriteSize());
  TRITONSORT_ASSERT(offset + length <= getWriteSize(),
         "Cannot partialSerialize() at offset %llu for %llu bytes because "
         "tuple only has %llu bytes", offset, length, getWriteSize());

  if (tupleStart != NULL &&
      ((readHeader && writeHeader) || (!readHeader && !writeHeader))) {
    // The tuple exists in contiguous memory and the read and write formats are
    // the same, so do partial copies offset from the start of the tuple.
    memcpy(destination, tupleStart + offset, length);
  } else if (tupleStart != NULL && readHeader && !writeHeader) {
    // The tuple exists in contiguous memory and we have a header, but we're not
    // writing it, so do partial copies offset from the start of the key.
    memcpy(destination, _key + offset, length);
  } else {
    // If we don't have contiguous memory or we have to synthesize a header out
    // of nothing, then this becomes a lot harder to implement, but we don't
    // expect to ever need this case, so simply abort.
    ABORT("partialSerialize() does not cover cases where tuple does not exist "
          "in contiguous memory or header is not read but must be written.");
  }
}

void KeyValuePair::deserialize(uint8_t* source) {
  tupleStart = source;

  if (readHeader) {
    _keyLength = KeyValuePair::keyLength(source);
    _key = KeyValuePair::key(source);
    _valueLength = KeyValuePair::valueLength(source);
    _value = KeyValuePair::value(source);
  } else {
    // There's no header, so use the specified key and value lengths.
    _keyLength = fixedKeyLength;
    _key = source;
    _valueLength = fixedValueLength;
    _value = _key + fixedKeyLength;
  }
}
