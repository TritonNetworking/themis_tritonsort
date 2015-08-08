#include "GenRandomRatioDataMapFunction.h"
#include "core/Hash.h"

GenRandomRatioDataMapFunction::GenRandomRatioDataMapFunction(
  uint64_t _dataSize, uint64_t _valueDimension, uint64_t _mypeerid,
  uint64_t _numPeers)
  : dataSize(_dataSize), valueDimension(_valueDimension),
    mypeerid(_mypeerid), numPeers(_numPeers) {
}

void GenRandomRatioDataMapFunction::map(KeyValuePair& kvPair,
                                        KVPairWriterInterface& writer) {
  // attempt to generate a total of 'dataSize' amount of random data
  // 'valueDimension' is used to tune tuple size

  uint64_t *pMapperID = (uint64_t *)kvPair.getKey();
  uint64_t *pNumMappers = (uint64_t *)kvPair.getValue();

  // bits: 64 (key) + 64 * valueDimension (value) + 32 (keylen) + 32 (valuelen)
  uint64_t sizePerPoint = 8 * (valueDimension + 2);  // in bytes
  uint64_t numPoints = dataSize / sizePerPoint;

  // partition at node level
  uint64_t numPointsPerPeer = numPoints / numPeers;
  if(numPoints % numPeers != 0){
    numPointsPerPeer++;
  }
  uint64_t nodeBeginningPoint = mypeerid * numPointsPerPeer;
  uint64_t nodeEndingPoint = nodeBeginningPoint + numPointsPerPeer;
  if(nodeEndingPoint > numPoints){
    nodeEndingPoint = numPoints;
  }

  // partition at mapper level
  uint64_t numPointsAtThisNode = nodeEndingPoint - nodeBeginningPoint;
  uint64_t numPointsPerMapper = numPointsAtThisNode / (*pNumMappers);
  if(numPointsAtThisNode % (*pNumMappers) != 0){
    numPointsPerMapper++;
  }
  uint64_t beginningPoint = nodeBeginningPoint
                             + (*pMapperID) * numPointsPerMapper;

  uint64_t endingPoint = beginningPoint + numPointsPerMapper;
  if(endingPoint > nodeEndingPoint){
    endingPoint = nodeEndingPoint;
  }

  unsigned short nrandBuf[3];
  nrandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  nrandBuf[1] = (unsigned short) getpid() ;
  nrandBuf[2] = (unsigned short) *pMapperID;

  uint64_t *outputValue = new uint64_t[valueDimension];


  for(uint64_t i = beginningPoint; i < endingPoint; i++){

    for(uint64_t j = 0; j < valueDimension; j++){
      outputValue[j] = nrand48(nrandBuf);
    }

    KeyValuePair outputKVPair;
    uint64_t key = Hash::hash(i);

    outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&key),
                                                sizeof(uint64_t));
    outputKVPair.setValue(reinterpret_cast<const uint8_t *>(outputValue),
                                 (valueDimension) * sizeof(uint64_t));

    writer.write(outputKVPair);
  }

  delete[] outputValue;
}
