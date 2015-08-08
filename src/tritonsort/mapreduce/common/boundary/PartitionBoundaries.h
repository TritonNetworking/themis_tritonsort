#ifndef THEMIS_PARTITION_BOUNDARIES_H
#define THEMIS_PARTITION_BOUNDARIES_H

#include <stdint.h>
#include <utility>

/**
   A simple struct that stores the boundaries of a given partition and allows
   the user to query if a given key is between the boundaries
 */
class PartitionBoundaries {
public:
  /// Constructor
  /**
     \param lowerBoundKey the key that forms the lower bound of this partition

     \param lowerBoundKeyLength the length of lowerBoundKey

     \param upperBoundKey the key that forms the upper bound of this partition

     \param upperBoundKeyLength the length of upperBoundKey
   */
  PartitionBoundaries(
    uint8_t* lowerBoundKey, uint32_t lowerBoundKeyLength,
    uint8_t* upperBoundKey, uint32_t upperBoundKeyLength);

  virtual ~PartitionBoundaries();

  /// Determine if a partition is within the boundaries defined by this object
  /**
     For the key to be within boundaries, it must be >= the lower bound key and
     < the upper bound key, if an upper bound key exists.

     /param key the key that is being checked
     /param keyLength the length of the key
   */
  bool keyIsWithinBoundaries(const uint8_t* key, uint32_t keyLength) const;

  std::pair<const uint8_t*, uint32_t> getLowerBoundKey() const;
  std::pair<const uint8_t*, uint32_t> getUpperBoundKey() const;
private:
  const uint32_t lowerBoundKeyLength;
  const uint32_t upperBoundKeyLength;

  uint8_t* lowerBoundKey;
  uint8_t* upperBoundKey;
};


#endif // THEMIS_PARTITION_BOUNDARIES_H
