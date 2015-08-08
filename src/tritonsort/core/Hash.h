#ifndef TRITONSORT_HASH_H
#define TRITONSORT_HASH_H

#include "murmurhash/MurmurHash3.h"

/**
   Hash is a utility class that provides a basic interface to the MurmurHash3
   function.
 */
class Hash {
public:
  /**
     Compute a 64-bit hash using the x64 128 bit MurmurHash3 function. Drops the
     last 64 bits to produce a 64 bit result.  A seed of 0 is used when
     computing hashes.

     \param key the key to hash

     \param keyLength the length of the key

     \return a 64-bit hash of the key
   */
  static inline uint64_t hash(const uint8_t* key, uint32_t keyLength) {
    uint64_t buffer[2];
    // Use MurmurHash 3F with a seed of 0.
    MurmurHash3_x64_128(key, keyLength, 0, buffer);
    // Drop the last 64 bits.
    return buffer[0];
  }

  /**
     Compute a 64 bit hash from a 64-bit key.

     \param key a 64-bit key to hash

     \return a 64-bit hash of the key
   */
  static inline uint64_t hash(uint64_t key) {
    return hash(reinterpret_cast<uint8_t*>(&key), sizeof(key));
  }
};

#endif // TRITONSORT_HASH_H
