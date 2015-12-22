#ifndef THEMIS_BASE_64_H
#define THEMIS_BASE_64_H

// Bit shifting logic adapted from http://base64.sourceforge.net/b64.c
// and http://www.adp-gmbh.ch/cpp/common/base64.html. Credit to Bob Trower and
// René Nyffenegger.
//
// See Base64_Trower_LICENSE and Base64_Nyffenegger_LICENSE for license
// information.

#include <stdint.h>

class Base64 {
public:
  virtual ~Base64() {}

  /// Base64-encode a region of memory
  /**
     \param in the memory region to encode

     \param length the length of `in` in bytes

     \param out the memory region to which to write the Base64-encoded form of
     `in`
   */
  static void encode(const uint8_t* in, uint64_t length, uint8_t* out);

  /// Base64-decode a region of memory
  /**
     \param in the memory region to decode

     \param length the length of `in` in bytes

     \param out the memory region to which to write the Base64-decoded form of
     `in`

     \return the number of bytes decoded
   */
  static uint64_t decode(const uint8_t* in, uint64_t length, uint8_t* out);

private:
  static const uint8_t encodeTable[64];
  static const uint8_t decodeTable[80];
};


#endif // THEMIS_BASE_64_H
