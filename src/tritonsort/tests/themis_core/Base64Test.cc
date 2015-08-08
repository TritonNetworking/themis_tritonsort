#include <string.h>

#include "core/Base64.h"
#include "tests/themis_core/Base64Test.h"

const char* Base64Test::testString = "Man is distinguished, not only by his "
  "reason, but by this singular passion from other animals, which is a lust "
  "of the mind, that by a perseverance of delight in the continued and "
  "indefatigable generation of knowledge, exceeds the short vehemence of "
  "any carnal pleasure.";

void Base64Test::testAgainstKnownEncode() {
  const uint8_t* testBytes = reinterpret_cast<const uint8_t*>(testString);

  uint64_t length = strlen(testString);

  uint8_t* output = new uint8_t[length * 2];
  memset(output, 0, length * 2);

  Base64::encode(testBytes, length, output);
  uint64_t outputLen = strlen(reinterpret_cast<char*>(output));

  const char* expected = "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyBy"
    "ZWFzb24sIGJ1dCBieSB0aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLC"
    "B3aGljaCBpcyBhIGx1c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2Yg"
    "ZGVsaWdodCBpbiB0aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2"
    "Yga25vd2xlZGdlLCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBw"
    "bGVhc3VyZS4=";
  uint64_t expectedLen = strlen(expected);

  CPPUNIT_ASSERT_EQUAL(expectedLen, outputLen);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(expected, output, expectedLen));

  memset(output, 0, length * 2);

  Base64::encode(testBytes, length - 1, output);

  CPPUNIT_ASSERT_EQUAL(0, memcmp(expected, output, expectedLen - 3));

  CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>('Q'), output[expectedLen - 3]);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>('='), output[expectedLen - 2]);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>('='), output[expectedLen - 1]);

  memset(output, 0, length * 2);

  Base64::encode(testBytes, length - 2, output);
  outputLen = strlen(reinterpret_cast<char*>(output));

  CPPUNIT_ASSERT_EQUAL(expectedLen - 4, outputLen);

  CPPUNIT_ASSERT_EQUAL(0, memcmp(expected, output, outputLen));

  memset(output, 0, length * 2);

  Base64::encode(
    reinterpret_cast<const uint8_t*>(testString), length - 3, output);
  outputLen = strlen(reinterpret_cast<char*>(output));

  CPPUNIT_ASSERT_EQUAL(expectedLen - 4, outputLen);

  CPPUNIT_ASSERT_EQUAL(0, memcmp(expected, output, outputLen - 2));

  CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>('U'), output[outputLen - 2]);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>('='), output[outputLen - 1]);

  delete[] output;
}

void Base64Test::testEncodeAndDecode() {
  const uint8_t* testBytes = reinterpret_cast<const uint8_t*>(testString);
  uint64_t testStringLength = strlen(testString);

  uint8_t* encoded = new uint8_t[testStringLength * 2];
  uint8_t* decoded = new uint8_t[testStringLength * 2];

  for (uint64_t testLength = 1; testLength <= testStringLength; testLength++) {
    memset(encoded, 0, testStringLength * 2);
    memset(decoded, 0, testStringLength * 2);

    Base64::encode(testBytes, testLength, encoded);
    uint64_t encodedLength = strlen(reinterpret_cast<char*>(encoded));

    uint64_t bytesDecoded = Base64::decode(encoded, encodedLength, decoded);

    uint64_t decodedLength = strlen(reinterpret_cast<char*>(decoded));

    CPPUNIT_ASSERT_EQUAL(bytesDecoded, decodedLength);
    CPPUNIT_ASSERT_EQUAL(testLength, decodedLength);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(decoded, testString, decodedLength));
  }

  delete[] encoded;
  delete[] decoded;
}

void Base64Test::testDecodeAllCodeLetters() {
  const char* testString = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz+/abc=";

  const uint8_t* testBytes = reinterpret_cast<const uint8_t*>(testString);
  uint64_t testStringLength = strlen(testString);

  uint8_t* encoded = new uint8_t[testStringLength * 2];
  uint8_t* decoded = new uint8_t[testStringLength * 2];

  uint64_t bytesDecoded = Base64::decode(testBytes, testStringLength, decoded);
  Base64::encode(decoded, bytesDecoded, encoded);

  CPPUNIT_ASSERT_EQUAL(
    strlen(reinterpret_cast<char*>(encoded)), testStringLength);

  CPPUNIT_ASSERT_EQUAL(0, memcmp(encoded, testString, testStringLength));

  delete[] encoded;
  delete[] decoded;
}
