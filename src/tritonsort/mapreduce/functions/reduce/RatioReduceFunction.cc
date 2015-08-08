#include <sys/types.h>
#include <unistd.h>

#include "RatioReduceFunction.h"
#include "core/Hash.h"
#include "core/Timer.h"
#include "mapreduce/common/KeyValuePair.h"

RatioReduceFunction::RatioReduceFunction(double _ratio)
  : ratio(_ratio) {
}

void RatioReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  unsigned short nrandBuf[3];
  nrandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  nrandBuf[1] = (unsigned short) getpid() ;
  nrandBuf[2] = (unsigned short) 3;

  unsigned short erandBuf[3];
  erandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  erandBuf[1] = (unsigned short) getpid() ;
  erandBuf[2] = (unsigned short) 5;

  while (iterator.next(kvPair)) {
    uint64_t repeat = (uint64_t) ratio;
    double frac = ratio - (double) repeat;
    // one more kvPair with probability "frac"
    // so that in expectation we get the correct 'ratio'
    if(erand48(erandBuf) < frac){
      repeat++;
    }

    // emit random output KVPairs so that the shuffle/output data is 'ratio'
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
}
