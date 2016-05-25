#include "core/ByteOrder.h"
#include "tests/themis_core/ByteOrderTest.h"

ByteOrderTest::ByteOrderTest() :
  deadbeef(0xDEADBEEF),
  deadbeefBigEndian(0),
  deadbeefLittleEndian(0) {
  // Initialize big endian and little endian representations of 0xDEADBEEF
  reinterpret_cast<uint8_t*>(&deadbeefBigEndian)[4] = 0xDE;
  reinterpret_cast<uint8_t*>(&deadbeefBigEndian)[5] = 0xAD;
  reinterpret_cast<uint8_t*>(&deadbeefBigEndian)[6] = 0xBE;
  reinterpret_cast<uint8_t*>(&deadbeefBigEndian)[7] = 0xEF;
  reinterpret_cast<uint8_t*>(&deadbeefLittleEndian)[3] = 0xDE;
  reinterpret_cast<uint8_t*>(&deadbeefLittleEndian)[2] = 0xAD;
  reinterpret_cast<uint8_t*>(&deadbeefLittleEndian)[1] = 0xBE;
  reinterpret_cast<uint8_t*>(&deadbeefLittleEndian)[0] = 0xEF;

  // Initialize the host order of 0xDEADBEEF
#ifdef BOOST_LITTLE_ENDIAN
  deadbeefHostOrder = deadbeefLittleEndian;
#elif BOOST_BIG_ENDIAN
  deadbeefHostOrder = deadbeefBigEndian;
#else // BOOST_LITTLE_ENDIAN
#error Neither BOOST_LITTLE_ENDIAN nor BOOST_BIG_ENDIAN defined.
#endif // BOOST_LITTLE_ENDIAN
}

TEST_F(ByteOrderTest, testHostToNetwork) {
  // Sanity check that the host order is correct.
  EXPECT_EQ(deadbeefHostOrder, deadbeef);

  // Convert to network order and check endianness.
  uint64_t networkOrder = hostToBigEndian64(deadbeef);
  EXPECT_EQ(deadbeefBigEndian, networkOrder);
}

TEST_F(ByteOrderTest, testNetworkToHost) {
  // Convert the big endian representation to host order.
  uint64_t actualHostOrder = bigEndianToHost64(deadbeefBigEndian);

  // Check that the actual host order returned by the conversion function is the
  // expected host order initialized in ByteOrderTest().
  EXPECT_EQ(deadbeefHostOrder, actualHostOrder);
}

TEST_F(ByteOrderTest, testCyclicConversion) {
  // Convert 0xC0FFEE from host to network and back to host.
  uint64_t coffee = 0xC0FFEE;

  uint64_t networkOrder = hostToBigEndian64(coffee);
#ifdef BOOST_LITTLE_ENDIAN
  // Host should be little endian, so check that the conversion changed coffee.
  uint64_t reversedCoffee = 0xEEFFC00000000000;
  EXPECT_EQ(reversedCoffee, networkOrder);
#elif BOOST_BIG_ENDIAN
  // Host should be big endian, so check that the conversion left coffee
  // unchanged.
  EXPECT_EQ(coffee, networkOrder);
#else // BOOST_LITTLE_ENDIAN
#error Neither BOOST_LITTLE_ENDIAN nor BOOST_BIG_ENDIAN defined.
#endif // BOOST_LITTLE_ENDIAN

  // Now convert back to host order.
  uint64_t hostOrder = bigEndianToHost64(networkOrder);

  // Check that the cyclic conversion returned 0xC0FFEE
  EXPECT_EQ(coffee, hostOrder);
}
