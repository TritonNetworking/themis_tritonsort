#ifndef THEMIS_KEY_VALUE_PAIR_H
#define THEMIS_KEY_VALUE_PAIR_H

#include <stdint.h>
#include <stdlib.h>

#include "core/Comparison.h"
#include "core/constants.h"

class KeyValuePair {
private:
  struct KeyValuePairHeader {
    uint32_t keyLength;
    uint32_t valueLength;
  };
public:
  // Header size constants:
  /// Key length is stored as uint32_t - 4 bytes
  static const uint64_t KEY_HEADER_SIZE = sizeof(uint32_t);
  /// Value length is stored as uint32_t - 4 bytes
  static const uint64_t VALUE_HEADER_SIZE = sizeof(uint32_t);
  /// Value is the last field in the header
  static const uint64_t HEADER_SIZE = KEY_HEADER_SIZE + VALUE_HEADER_SIZE;

  /// Constructor
  KeyValuePair();

  /// Destructor
  virtual ~KeyValuePair();

  /**
     Get a pointer to a tuple's key.

     \param tuple a byte pointer to the start of the tuple

     \return a pointer to the start of the tuple's key
   */
  static inline uint8_t* key(uint8_t* tuple) {
    return tuple + HEADER_SIZE;
  }

  /**
     Get a pointer to the key of a tuple without a header without leaking the
     abstraction.

     \param tuple a byte pointer to the start of the tuple

     \return a pointer to the start of the tuple's key
   */
  static inline uint8_t* keyWithoutHeader(uint8_t* tuple) {
    return tuple;
  }

  /**
     Get a pointer to a tuple's value.

     \param tuple a byte pointer to the start of the tuple

     \return a pointer to the start of the tuple's value
   */
  static inline uint8_t* value(uint8_t* tuple) {
    return key(tuple) + keyLength(tuple);
  }

  /**
     Get a pointer to the value of a tuple without a header without leaking the
     abstraction.

     \param tuple a byte pointer to the start of the tuple

     \param keyLength the length of the key

     \return a pointer to the start of the tuple's value
   */
  static inline uint8_t* valueWithoutHeader(
    uint8_t* tuple, uint32_t keyLength) {
    return tuple + keyLength;
  }

  /**
     Given a tuple, get the next tuple in a contiguous region of memory.

     \param tuple a byte pointer to the start of the tuple

     \return a pointer to the start of the next tuple
   */
  static inline uint8_t* nextTuple(uint8_t* tuple) {
    return tuple + tupleSize(tuple);
  }

  /**
     Get the length of a tuple's key.

     \param tuple a byte pointer to the start of the tuple

     \return the length of the tuple's key
   */
  static inline uint32_t keyLength(uint8_t* tuple) {
    return reinterpret_cast<KeyValuePairHeader*>(tuple)->keyLength;
  }

  static inline void setKeyLength(uint8_t* tuple, uint32_t keyLength) {
    reinterpret_cast<KeyValuePairHeader*>(tuple)->keyLength = keyLength;
  }

  /**
     Get the length of a tuple's value.

     \param tuple a byte pointer to the start of the tuple

     \return the length of the tuple's value
   */
  static inline uint32_t valueLength(uint8_t* tuple) {
    return reinterpret_cast<KeyValuePairHeader*>(tuple)->valueLength;
  }

  static inline uint64_t tupleSize(
    uint64_t keyLength, uint64_t valueLength, bool useHeader=true) {
    if (useHeader) {
      return HEADER_SIZE + keyLength + valueLength;
    } else {
      return keyLength + valueLength;
    }
  }

  static inline uint64_t tupleSize(uint8_t* tuple) {
    return tupleSize(keyLength(tuple), valueLength(tuple));
  }

  static inline void setValueLength(uint8_t* tuple, uint32_t valueLength) {
    reinterpret_cast<KeyValuePairHeader*>(tuple)->valueLength =
      valueLength;
  }

  static inline int compareByKey(
    const KeyValuePair& tuple1, const KeyValuePair& tuple2) {

    return compare(
      tuple1._key, tuple1._keyLength, tuple2._key, tuple2._keyLength);
  }

  static inline int compareByValue(
    const KeyValuePair& tuple1, const KeyValuePair& tuple2) {
    return compare(
      tuple1._value, tuple1._valueLength, tuple2._value, tuple2._valueLength);
  }

  /// Get key and value pointers for a tuple residing in a memory region.
  /**
     \param memoryRegion a raw byte buffer that will contain a tuple

     \param keyLength the length of the tuple's key

     \param[out] key a pointer to the tuple's key

     \param[out] value a pointer to the tuple's value

     \param writeWithoutHeader if true, consider the tuple to be written
     without header information
   */
  static inline void getKeyValuePointers(
    uint8_t* memoryRegion, uint32_t keyLength, uint8_t*& key, uint8_t*&value,
    bool writeWithoutHeader=false) {
    if (writeWithoutHeader) {
      key = keyWithoutHeader(memoryRegion);
      value = valueWithoutHeader(memoryRegion, keyLength);
    } else {
      // Set the key length in the tuple's header
      setKeyLength(memoryRegion, keyLength);
      // Using that header, pull out the key and value pointers
      key = KeyValuePair::key(memoryRegion);
      value = KeyValuePair::value(memoryRegion);
    }
  }

  /// \return a pointer to the key
  inline const uint8_t* getKey() const {
    return _key;
  }

  /// \return a pointer to the value
  inline const uint8_t* getValue() const {
    return _value;
  }

  /// \return the length of the key
  inline uint32_t getKeyLength() const {
    return _keyLength;
  }

  /// \return the length of the value
  inline uint32_t getValueLength() const {
    return _valueLength;
  }

  inline void setKey(const uint8_t* key, uint32_t keyLength) {
    // Set tuple start pointer to NULL since the tuple isn't necessarily
    // going to be contiguous in memory.
    tupleStart = NULL;
    _key = key;
    _keyLength = keyLength;
  }

  inline void setValue(const uint8_t* value, uint32_t valueLength) {
    // Set tuple start pointer to NULL since the tuple isn't necessarily
    // going to be contiguous in memory.
    tupleStart = NULL;
    _value = value;
    _valueLength = valueLength;
  }

  inline uint64_t getReadSize() const {
    if (readHeader) {
      return HEADER_SIZE + _keyLength + _valueLength;
    } else {
      return fixedKeyLength + fixedValueLength;
    }
  }

  inline uint64_t getWriteSize() const {
    if (writeHeader) {
      return HEADER_SIZE + _keyLength + _valueLength;
    } else {
      return _keyLength + _valueLength;
    }
  }

  /**
     \param writeWithoutHeader if true, write tuples without headers
   */
  void setWriteWithoutHeader(bool writeWithoutHeader);

  /**
     \return true if we're writing tuples without headers
   */
  bool getWriteWithoutHeader();

  /// Instruct the tuple to deserialize itself without header bytes.
  /**
     \param keyLength read this many bytes for the key

     \param valueLength read this many bytes for the value
   */
  void readWithoutHeader(uint32_t keyLength, uint32_t valueLength);

  /// Serialize the header to a destination buffer
  /**
     \param destination the raw buffer to serialize the header to

     \return the number of bytes serialized
   */
  uint64_t serializeHeader(uint8_t* destination) const;

  /// Serialize the entire tuple to a destination buffer
  /**
     \param destination the raw buffer to serialize tuple to
  */
  void serialize(uint8_t* destination) const;

  /// Serialize a specific number of bytes a specific offset into the tuple
  /// to a destination buffer.
  /**
     \param destination the raw buffer to serialize tuple to

     \param offset the offset into the tuple where we want to begin
     serialization

     \param length the number of bytes to serialize
   */
  void partialSerialize(
    uint8_t* destination, uint64_t offset, uint64_t length) const;

  /// Deserialize a tuple from a source buffer
  /**
     \param source the raw buffer to deserialze from
   */
  void deserialize(uint8_t* source);

private:
  bool readHeader;
  bool writeHeader;

  uint32_t fixedKeyLength;
  uint32_t fixedValueLength;

  /// If we're deserializing a tuple from a buffer then we know it exists
  /// contiguously in memory. In this case we can eliminate some memcpy
  /// calls by storing the start of the tuple and copying all the data in one
  /// shot.
  const uint8_t* tupleStart;

  uint32_t _keyLength;
  const uint8_t* _key;
  uint32_t _valueLength;
  const uint8_t* _value;
};


#endif // THEMIS_KEY_VALUE_PAIR_H
