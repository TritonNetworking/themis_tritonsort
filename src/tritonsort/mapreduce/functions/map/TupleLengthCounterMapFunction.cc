#include "TupleLengthCounterMapFunction.h"
#include "mapreduce/common/KeyValuePair.h"

const uint64_t TupleLengthCounterMapFunction::COUNT = 1;
const uint8_t TupleLengthCounterMapFunction::KEY = 0x00;
const uint8_t TupleLengthCounterMapFunction::VALUE = 0x01;
const uint8_t TupleLengthCounterMapFunction::TUPLE = 0x02;

void TupleLengthCounterMapFunction::map(KeyValuePair& kvPair,
                                        KVPairWriterInterface& writer) {
  KeyValuePair keyOutputKVPair, valueOutputKVPair, tupleOutputKVPair;
  uint32_t keyLength, valueLength, tupleLength;

  keyLength = kvPair.getKeyLength();
  valueLength = kvPair.getValueLength();
  tupleLength = KeyValuePair::tupleSize(keyLength, valueLength);

  /* The tuples we generate have a one byte type (key, value, or whole tuple)
   * followed by the actual value, which in Themis is 4 bytes */
  const uint8_t KEYLEN = 1 + sizeof(keyLength);
  uint8_t key[KEYLEN];

  memcpy(key, &KEY, 1);
  memcpy(key+1, &keyLength, sizeof(keyLength));
  keyOutputKVPair.setKey(key, KEYLEN);
  keyOutputKVPair.setValue(reinterpret_cast<const uint8_t*>(&COUNT),
                           sizeof(COUNT));
  writer.write(keyOutputKVPair);

  memcpy(key, &VALUE, 1);
  memcpy(key+1, &valueLength, sizeof(valueLength));
  valueOutputKVPair.setKey(key, KEYLEN);
  valueOutputKVPair.setValue(reinterpret_cast<const uint8_t*>(&COUNT),
                             sizeof(COUNT));
  writer.write(valueOutputKVPair);

  memcpy(key, &TUPLE, 1);
  memcpy(key+1, &tupleLength, sizeof(tupleLength));
  tupleOutputKVPair.setKey(key, KEYLEN);
  tupleOutputKVPair.setValue(reinterpret_cast<const uint8_t*>(&COUNT),
                             sizeof(COUNT));
  writer.write(tupleOutputKVPair);
}
