#include "GenRandomKMeansDataMapFunction.h"
#include "core/Hash.h"

GenRandomKMeansDataMapFunction::GenRandomKMeansDataMapFunction(
  uint64_t _numPoints, uint64_t _dimension, uint64_t _maxPointCoord,
  uint64_t _k, std::string _kCentersURL, uint64_t _mypeerid, uint64_t _numPeers)
  : numPoints(_numPoints), dimension(_dimension), maxPointCoord(_maxPointCoord),
    k(_k), kCentersURL(_kCentersURL),
    mypeerid(_mypeerid), numPeers(_numPeers) {
}


/**
   map function is run once on each peer
   Each peer responsible to generate for approximately
      (numPoints / numPeers) Points, in order of 'mypeerid'

   map() function assumes that kvPair contains <mapperID, numMappers>
      for partitioning the network generator jobs in parallel

 */
void GenRandomKMeansDataMapFunction::map(KeyValuePair& kvPair,
                                         KVPairWriterInterface& writer) {
  uint64_t *pMapperID = (uint64_t *)kvPair.getKey();
  uint64_t *pNumMappers = (uint64_t *)kvPair.getValue();

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

  uint64_t *outputValue = new uint64_t[dimension];


  for(uint64_t i = beginningPoint; i < endingPoint; i++){

    for(uint64_t j = 0; j < dimension; j++){
      outputValue[j] = nrand48(nrandBuf) % maxPointCoord;
    }

    KeyValuePair outputKVPair;
    uint64_t key = Hash::hash(i);

    outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&key),
                                                sizeof(uint64_t));
    outputKVPair.setValue(reinterpret_cast<const uint8_t *>(outputValue),
                                 (dimension) * sizeof(uint64_t));

    writer.write(outputKVPair);
  }

  // also initialize k centers on the first node
  if(mypeerid == 0 && *pMapperID == 0){
    std::ofstream outfile(kCentersURL.c_str());
    ABORT_IF(!outfile.is_open(), "Cannot open file %s\n", kCentersURL.c_str());
    for(uint64_t i = 0; i < k; i++){
      outfile << i << " " << Hash::hash(i);
      for(uint64_t j = 0; j < dimension; j++){
        uint32_t coord = nrand48(nrandBuf) % maxPointCoord;
        outfile << " " << coord;
      }
      outfile << std::endl;
    }
    outfile.close();
  }

  delete[] outputValue;
}
