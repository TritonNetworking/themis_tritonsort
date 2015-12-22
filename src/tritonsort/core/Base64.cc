#include <algorithm>

#include "core/Base64.h"
#include "core/TritonSortAssert.h"

// Bit shifting logic adapted from http://base64.sourceforge.net/b64.c
// and http://www.adp-gmbh.ch/cpp/common/base64.html. Credit to Bob Trower and
// René Nyffenegger.
//
// See Base64_Trower_LICENSE and Base64_Nyffenegger_LICENSE for license
// information.

const uint8_t Base64::encodeTable[] = {
  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
  'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
  'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
  't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
  '8', '9', '+', '/'
};

/**
   Subtracting 43 from all valid ASCII code letters gives the following mapping:
   Code letter = subtracted ASCII value (base64-encoding value)

   + = 0 (62)
   / = 4 (63)
   0-9 = 5-14 (52-61)
   A-Z = 22-47 (0-25)
   a-z = 54-79 (26-51)

   Any characters marked as 64 are invalid translations (essentially padding
   characters to make the translation easier)
 */
const uint8_t Base64::decodeTable[] = {
  62, 64, 64, 64, 63, 52, 53, 54, 55, 56,
  57, 58, 59, 60, 61, 64, 64, 64, 64, 64,
  64, 64,  0,  1,  2,  3,  4,  5,  6,  7,
  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
  18, 19, 20, 21, 22, 23, 24, 25, 64, 64,
  64, 64, 64, 64, 26, 27, 28, 29, 30, 31,
  32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
  42, 43, 44, 45, 46, 47, 48, 49, 50, 51
};


void Base64::encode(const uint8_t* in, uint64_t length, uint8_t* out) {
  uint8_t* outPtr = out;
  const uint8_t* inPtr = in;
  uint64_t remaining = length;

  // Let the bit-twiddling COMMENCE!
  while (remaining > 0) {
    uint8_t in0 = inPtr[0];
    uint8_t in1 = 0;
    uint8_t in2 = 0;

    if (remaining > 1) {
      in1 = inPtr[1];
    }

    if (remaining > 2) {
      in2 = inPtr[2];
    }

    int index0 = (in0 & 0xfc) >> 2;
    int index1 = ((in0 & 0x03) << 4) | ((in1 & 0xf0) >> 4);

    outPtr[0] = encodeTable[index0];
    outPtr[1] = encodeTable[index1];

    if (remaining > 1) {
      int index2 = ((in1 & 0x0f) << 2) | ((in2 & 0xc0) >> 6);
      outPtr[2] = encodeTable[index2];
    } else {
      outPtr[2] = static_cast<uint8_t>('=');
    }

    if (remaining > 2) {
      int index3 = in2 & 0x3f;
      outPtr[3] = encodeTable[index3];
    } else {
      outPtr[3] = static_cast<uint8_t>('=');
    }

    outPtr += 4;
    inPtr += 3;
    remaining -= std::min<uint64_t>(3, remaining);
  }
}

uint64_t Base64::decode(const uint8_t* in, uint64_t length, uint8_t* out) {
  uint8_t* outPtr = out;
  const uint8_t* inPtr = in;
  uint64_t remaining = length;

  uint8_t currIn[4];

  while (remaining > 0) {
    uint64_t numOutputBytes = 3;

    for (uint64_t i = 0; i < 4; i++) {
      uint8_t x = inPtr[i];

      if (x == '=') {
        x = 0;
        numOutputBytes--;
      } else if (x < 43 || x > 122) {
        // We should not have encountered this character, since it's not one of
        // the valid ASCII symbols for a Base64-encoded string
        x = 64;
      } else {
        x = decodeTable[x - 43];
      }

      ABORT_IF(x == 64, "Encountered invalid character '%c' while "
               "Base64-decoding", x);
      currIn[i] = x;
    }

    outPtr[0] = (currIn[0] << 2) + ((currIn[1] & 0x30) >> 4);

    if (numOutputBytes > 1) {
      outPtr[1] = ((currIn[1] & 0xf) << 4) + ((currIn[2] & 0x3c) >> 2);
    }

    if (numOutputBytes > 2) {
      outPtr[2] = ((currIn[2] & 0x3) << 6) + currIn[3];
    }

    outPtr += numOutputBytes;
    inPtr += 4;
    remaining -= std::min<uint64_t>(4, remaining);
  }

  return outPtr - out;
}
