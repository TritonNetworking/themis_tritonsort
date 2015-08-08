#include <boost/random/mersenne_twister.hpp>
#include <boost/random.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <boost/random/variate_generator.hpp>

#include "GenRandomNetworkMapFunction.h"
#include "core/Hash.h"

GenRandomNetworkMapFunction::GenRandomNetworkMapFunction(
  uint64_t _numVertices, double _edgeProbability, uint64_t _maxNeighbors,
  uint64_t _mypeerid, uint64_t _numPeers)
  : numVertices(_numVertices), edgeProbability(_edgeProbability),
    maxNeighbors(_maxNeighbors), mypeerid(_mypeerid), numPeers(_numPeers) {
}


/**
   Each mapper should run exactly one instance of this map function; we assume
   that the function is kick-started by a single <mapper ID / # mappers> pair.

   Each map function is responsible for generating the adjacency lists for
   a disjoint set of approximately (numVertices / numPeers) vertices.

   The vertex structures output by the map function are an array of the
   following form:

   <numNeighbors (uint64_t), firstNeighborID (uint64_t), ..., lastNeighborID
   (uint64_t), vertexPageRankValue (double)>

 */
void GenRandomNetworkMapFunction::map(KeyValuePair& kvPair,
                                      KVPairWriterInterface& writer) {
  uint64_t *pMapperID = (uint64_t*) kvPair.getKey();
  uint64_t *pNumMappers = (uint64_t*) kvPair.getValue();

  uint64_t maxAdjListSize = (numVertices < maxNeighbors) ?
                                              numVertices : maxNeighbors;

  // Need two additional slots to store the number of neighbors and the node's
  // initial PageRank
  uint64_t *outputValue = new uint64_t[maxAdjListSize + 2];

  srand48(Timer::posixTimeInMicros() * getpid());

  // partition at node level Determine the portion of the vertex space for
  // which this node is responsible
  uint64_t numVerticesPerPeer = numVertices / numPeers;
  if (numVertices % numPeers != 0) {
    numVerticesPerPeer++;
  }

  uint64_t nodeBeginningVertex = mypeerid * numVerticesPerPeer;
  uint64_t nodeEndingVertex = nodeBeginningVertex + numVerticesPerPeer;

  if (nodeEndingVertex > numVertices) {
    nodeEndingVertex = numVertices;
  }

  // Determine the range of vertex IDs that this map function is responsible
  // for generating
  uint64_t numVerticesAtThisNode = nodeEndingVertex - nodeBeginningVertex;
  uint64_t numVerticesPerMapper = numVerticesAtThisNode / (*pNumMappers);
  if (numVerticesAtThisNode % (*pNumMappers) != 0) {
    numVerticesPerMapper++;
  }
  uint64_t beginningVertex = nodeBeginningVertex + (*pMapperID) *
    numVerticesPerMapper;

  uint64_t endingVertex = beginningVertex + numVerticesPerMapper;

  if (endingVertex > nodeEndingVertex) {
    endingVertex = nodeEndingVertex;
  }

  unsigned short nrandBuf[3];
  nrandBuf[0] = (unsigned short) Timer::posixTimeInMicros();
  nrandBuf[1] = (unsigned short) getpid();
  nrandBuf[2] = (unsigned short) *pMapperID;

  // We will draw the number of edges from a binomial distribution so that the
  // probability that any two vertices are connected by an edge is roughly
  // equal to edgeProbability

  boost::mt19937 mersenneTwister;
  boost::binomial_distribution<> binomialDistribution(
    numVertices, edgeProbability);
  boost::variate_generator<boost::mt19937&, boost::binomial_distribution<> >
    nextBinomialValue(mersenneTwister, binomialDistribution);

  for (uint64_t i = beginningVertex; i < endingVertex; i++) {
    uint64_t numEdges = 0;

    if ((int) edgeProbability == 1) {
      // If edge probability is 1, generate maxNeighbors random neighbors
      numEdges = maxAdjListSize;
    } else {
      // Draw the number of edges from the binomial distribution
      numEdges = std::min<uint64_t>(nextBinomialValue(), maxAdjListSize);
    }

    for (uint64_t j = 1; j <= numEdges; j++) {
      uint32_t neighbor = nrand48(nrandBuf) % numVertices;
      outputValue[j] = Hash::hash(neighbor);
    }

    double initialPageRank = 1.0 / numVertices;
    outputValue[0] = numEdges;
    memcpy(&outputValue[numEdges + 1], &initialPageRank, sizeof(double));

    KeyValuePair outputKVPair;
    uint64_t key = Hash::hash(i);

    outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&key),
                                                sizeof(uint64_t));
    outputKVPair.setValue(reinterpret_cast<const uint8_t *>(outputValue),
                                 (numEdges + 2) * sizeof(uint64_t));

    writer.write(outputKVPair);
  }

  delete[] outputValue;
}
