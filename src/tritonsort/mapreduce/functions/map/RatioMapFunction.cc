#include "RatioMapFunction.h"
#include "core/Hash.h"

RatioMapFunction::RatioMapFunction(double _ratio)
  : ratio(_ratio) {
}

void RatioMapFunction::map(KeyValuePair& kvPair,
                           KVPairWriterInterface& writer) {
  unsigned short nrandBuf[3];
  nrandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  nrandBuf[1] = (unsigned short) getpid() ;
  nrandBuf[2] = (unsigned short) 3;

  unsigned short erandBuf[3];
  erandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  erandBuf[1] = (unsigned short) getpid() ;
  erandBuf[2] = (unsigned short) 5;

  uint64_t repeat = (uint64_t) ratio;
  double frac = ratio - (double) repeat;
  // one more kvPair with probability "frac"
  // so that in expectation we get the correct 'ratio'
  if(erand48(erandBuf) < frac){
    repeat++;
  }

  // emit random immediate KVPairs so that the input/shuffle data is 'ratio'
  for(uint64_t i = 0; i < repeat; i++){
    KeyValuePair outputKVPair;

    uint64_t r = nrand48(nrandBuf);
    uint64_t key = Hash::hash(r);

    outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&key),
                                                sizeof(uint64_t));
    outputKVPair.setValue(kvPair.getValue(), kvPair.getValueLength());

    writer.write(outputKVPair);
  }

}
