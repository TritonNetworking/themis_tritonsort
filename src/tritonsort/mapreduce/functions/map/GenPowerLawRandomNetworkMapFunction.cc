#include <math.h>

#include "GenPowerLawRandomNetworkMapFunction.h"
#include "core/Hash.h"

GenPowerLawRandomNetworkMapFunction::GenPowerLawRandomNetworkMapFunction(
  uint64_t _numVertices, uint64_t _maxVertexDegree, uint64_t _minVertexDegree,
  uint64_t _mypeerid,  uint64_t _numPeers)
  : numVertices(_numVertices), maxVertexDegree(_maxVertexDegree),
   minVertexDegree(_minVertexDegree), mypeerid(_mypeerid), numPeers(_numPeers) {
}


/**
   map function is run once on each peer
   Each peer responsible to generate adjacency lists for approximately
      (numVertices / numPeers) vertices, in order of 'mypeerid'

   map() function assumes that kvPair contains <mapperID, numMappers>
      for partitioning the network generator jobs in parallel

 */
void GenPowerLawRandomNetworkMapFunction::map(KeyValuePair& kvPair,
                                              KVPairWriterInterface& writer) {
  uint64_t *pMapperID = (uint64_t *)kvPair.getKey();
  uint64_t *pNumMappers = (uint64_t *)kvPair.getValue();

  // compute power law degree distribution
  uint64_t *degreeEdgeCDF = new uint64_t[maxVertexDegree + 1];
  uint64_t *degreeVertexCDF = new uint64_t[maxVertexDegree + 1];
  for(uint64_t i = 0; i < minVertexDegree; i++){
    degreeEdgeCDF[i] = 0;
    degreeVertexCDF[i] = 0;
  }

  double sum = 0.0;
  for(uint64_t i = minVertexDegree; i <= maxVertexDegree; i++){
    sum += 1.0 / (i * i);
  }

  uint64_t total = 0;
  for(uint64_t i = minVertexDegree; i <= maxVertexDegree; i++){
    double frac = (1.0 / (i * i)) / sum;

    // number of vertices with degree i
    uint64_t diff = floor(frac * numVertices + 0.5); // integer round

    degreeVertexCDF[i] = degreeVertexCDF[i-1] + diff;
    degreeEdgeCDF[i] = degreeEdgeCDF[i-1] + diff * (i + 1); // extra one
    total += diff;
  }

  // correct rounding error
  int64_t vertexAdjustment = numVertices - total;
  for(uint64_t i = minVertexDegree; i <= maxVertexDegree; i++){
    degreeVertexCDF[i] += vertexAdjustment;
    degreeEdgeCDF[i] += vertexAdjustment * (minVertexDegree + 1);
  }

  ABORT_IF(degreeVertexCDF[maxVertexDegree] != numVertices,
              "Invalid degree distribution");

  uint64_t totalNumEdges = degreeEdgeCDF[maxVertexDegree];

  // partition at node level
  uint64_t numEdgesPerPeer = totalNumEdges / numPeers;
  if(totalNumEdges % numPeers != 0){
    numEdgesPerPeer++;
  }
  uint64_t nodeBeginningEdge = mypeerid * numEdgesPerPeer;
  uint64_t nodeEndingEdge = nodeBeginningEdge + numEdgesPerPeer;
  if(nodeEndingEdge > totalNumEdges){
    nodeEndingEdge = totalNumEdges;
  }

  // partition at mapper level
  uint64_t numEdgesAtThisNode = nodeEndingEdge - nodeBeginningEdge;
  uint64_t numEdgesPerMapper = numEdgesAtThisNode / (*pNumMappers);
  if(numEdgesAtThisNode % (*pNumMappers) != 0){
    numEdgesPerMapper++;
  }
  uint64_t beginningEdge = nodeBeginningEdge
                             + (*pMapperID) * numEdgesPerMapper;

  uint64_t endingEdge = beginningEdge + numEdgesPerMapper;
  if(endingEdge > nodeEndingEdge){
    endingEdge = nodeEndingEdge;
  }

  // buffer for nrand48
  unsigned short nrandBuf[3];
  nrandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  nrandBuf[1] = (unsigned short) getpid() ;
  nrandBuf[2] = (unsigned short) *pMapperID;

  uint64_t curVertexDegree = minVertexDegree;
  for(uint64_t e = beginningEdge; e < endingEdge; e++){
    while(!(e < degreeEdgeCDF[curVertexDegree] &&
                         e >= degreeEdgeCDF[curVertexDegree - 1])){
      curVertexDegree++;
    }
    uint64_t v = degreeVertexCDF[curVertexDegree - 1] +
                    (e - degreeEdgeCDF[curVertexDegree - 1]) / (curVertexDegree + 1);
    uint64_t vID = Hash::hash(v);

    if((e - degreeEdgeCDF[curVertexDegree - 1]) % (curVertexDegree + 1) == 0){
      // emit itself -- to recover leaf vertex in reducer
      uint64_t zero = 0L;
      KeyValuePair outputKVPair;
      outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&vID),
          sizeof(uint64_t));
      outputKVPair.setValue(reinterpret_cast<const uint8_t *>(&zero),
          sizeof(uint64_t));
      writer.write(outputKVPair);
    } else {
      // emitting a directed link neighbor -> v
      uint32_t neighbor = nrand48(nrandBuf) % numVertices;
      uint64_t neighborID = Hash::hash(neighbor);
      KeyValuePair outputKVPair;
      outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&neighborID),
          sizeof(uint64_t));
      outputKVPair.setValue(reinterpret_cast<const uint8_t *>(&vID),
          sizeof(uint64_t));

      writer.write(outputKVPair);
    }
  }

  delete[] degreeVertexCDF;
  delete[] degreeEdgeCDF;
}
