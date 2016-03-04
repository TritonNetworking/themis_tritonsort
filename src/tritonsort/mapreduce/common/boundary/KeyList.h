#ifndef MAPRED_KEY_LIST_H
#define MAPRED_KEY_LIST_H

#include "core/constants.h"

class File;

/**
   KeyList is a data structure that holds partition boundary keys. It provides a
   a search function called findLowerBound() that determines the greatest lower
   bound of a key among the list of boundary keys. This function can be used to
   assign a partition ID to the key in question.
 */
class KeyList  {
public:
  /// Provides information about keys
  struct KeyInfo {
    uint32_t keyLength; /// the length of the key
    uint8_t* keyPtr; /// a pointer to the key itself
    KeyInfo()
      : keyLength(0),
        keyPtr(NULL) {}
  };

  /**
     Construct a new KeyList from a serialized representation. This function can
     is used to load KeyLists from disks.

     \param openFile an open file object to load the disk from.

     \warning openFile must be opened and closed by the caller. These semantics
     exist so that multiple KeyLists can be concatenated together in a single
     file.

     \return a new KeyList object loaded from the specified file
   */
  static KeyList* newKeyListFromFile(File& openFile);

  /// Constructor
  /**
     \param numKeys the number of keys in the key list

     \param numBytes the total number of bytes in all keys that will be added to
     the key list

     \param lowerBoundOffset a numerical offset that will be added to the value
     returned by findLowerBounder(). This value can be used to convert the index
     of the lower bound key to some globally unique value such as cluster-wide
     logical disk ID. This value defaults to 0.
   */
  KeyList(uint64_t numKeys, uint64_t numBytes, uint64_t lowerBoundOffset=0);

  /// Destructor
  virtual ~KeyList();

  /**
     Add a key to the KeyList.

     \param key the key to add

     \param keyLength the lenght of the key
   */
  void addKey(const uint8_t* key, uint32_t keyLength);

  /**
     Find the greatest lower bound of the specified key in the key list, and
     return its index. If lowerBoundOffset is non-zero, it is added to the
     result, so that this function can be used to return some globally unique
     ID for the specified key, such as partition ID (logical disk ID).

     \param key the key whose lower bound to retrieve

     \param keyLength the length of the key

     \return the index of the greatest lower bound of key in the KeyList,
     possibly with lowerBoundOffset added to it
   */
  uint64_t findLowerBound(const uint8_t* key, uint32_t keyLength) const;

  /**
     Write a KeyList to an opened file so that it can be persisted to disk.

     \param openFile an open file to write to

     \warning the file must be opened and closed by the caller. These semantics
     exist so that multiple KeyLists can be concatenated together in a single
     file.
   */
  void writeToFile(File& openFile) const;

  /**
     Check if this KeyList is the same as another.

     \param other the other KeyList to test for equality

     \return true if the two key lists contain the same keys (but not necesarily
     the same underlying memory buffers)
   */
  bool equals(const KeyList& other) const;

  /**
      \return the number of bytes in the internal array objects for accounting
      purposes.
   */
  uint64_t getCurrentSize() const;

  /**
     \return the (maximum) number of keys in the key list
   */
  uint64_t getNumKeys() const;

private:
  const uint64_t numKeys;
  const uint64_t numBytes;
  // When searching for lower bounds, add this value as an offset to the ID of
  // the lower bound key.
  const uint64_t lowerBoundOffset;

  // Store the keys contiguously for better cache locality.
  uint8_t* keyBuffer;
  std::vector<KeyInfo> keyInfos;

  // Used for adding keys
  uint64_t nextKeyOffset;
  uint64_t nextByteOffset;
};

#endif // MAPRED_KEY_LIST_H
