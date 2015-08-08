#include <boost/detail/endian.hpp>
#include <stdint.h>

/**
   Swap endian-ness of a 64-bit number. This function modifies its input, so it
   must pass by value. Used internally by hostToBigEndian64() and
   bigEndianToHost64().

   \param number a 64-bit number to reverse

   \return the number in opposite endian-ness
 */
inline uint64_t _swapBytes(uint64_t number) {
  uint8_t tempByte;

  // Swap bytes 0-7, 1-6, 2-5, 3-4.
  // We're expecting the compiler to optimize this loop out.
  for (uint64_t i = 0; i < 4; ++i) {
    tempByte = reinterpret_cast<uint8_t*>(&number)[i];
    reinterpret_cast<uint8_t*>(&number)[i] =
      reinterpret_cast<uint8_t*>(&number)[8 - i - 1];
    reinterpret_cast<uint8_t*>(&number)[8 - i - 1] = tempByte;
  }

  return number;
}

/**
   Convert a uint64_t from host to big endian byte order.

   \param number the uint64_t to convert

   \return the big endian representation of number
 */
inline uint64_t hostToBigEndian64(uint64_t number) {
#ifdef BOOST_LITTLE_ENDIAN
  return _swapBytes(number);
#elif BOOST_BIG_ENDIAN
  return number;
#else // BOOST_LITTLE_ENDIAN
#error Neither BOOST_LITTLE_ENDIAN nor BOOST_BIG_ENDIAN defined.
#endif // BOOST_LITTLE_ENDIAN
  return 0; // Should never happen.
}

/**
   Convert a uint64_t from big endian to host byte order.

   \param number the uint64_t to convert

   \return the host representation of number
 */
inline uint64_t bigEndianToHost64(uint64_t number) {
#ifdef BOOST_LITTLE_ENDIAN
  return _swapBytes(number);
#elif BOOST_BIG_ENDIAN
  return number;
#else // BOOST_LITTLE_ENDIAN
#error Neither BOOST_LITTLE_ENDIAN nor BOOST_BIG_ENDIAN defined.
#endif // BOOST_LITTLE_ENDIAN
  return 0; // Should never happen.
}
