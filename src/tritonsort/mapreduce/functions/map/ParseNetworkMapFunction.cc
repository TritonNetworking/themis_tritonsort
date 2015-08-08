#include <arpa/inet.h>
#include <string.h>
#include <string>
#include <stdlib.h>
#include <time.h>
#include "core/Params.h"
#include <openssl/sha.h>
#include <iostream>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <sstream>
#include <vector>


#include "ParseNetworkMapFunction.h"

ParseNetworkMapFunction::ParseNetworkMapFunction(uint64_t _numVertices,
                                                       std::string _fileURL,
                                                       uint64_t _maxNeighbors,
                                                       uint64_t _mypeerid,
                                                       uint64_t _numPeers)
  : numVertices(_numVertices), fileURL(_fileURL),
    maxNeighbors(_maxNeighbors), mypeerid(_mypeerid), numPeers(_numPeers) {
}

uint64_t sha1sum1(uint64_t in){

  char shaInBuf[17];
  unsigned char shaOutBuf[20];

  sprintf(shaInBuf, "%016lX", in);
  SHA1((const unsigned char *)shaInBuf, strlen(shaInBuf), shaOutBuf);

  uint64_t out;
  unsigned char *ptr = (unsigned char *) (&out);

  for(uint8_t i = 0; i < sizeof(uint64_t); i++){
    ptr[i] = shaOutBuf[i];
  }

  return out;
}

/**
   map function is run once on each peer
   Each peer responsible to generate adjacency lists for approximately
      (numVertices / numPeers) vertices, in order of 'mypeerid'

 */
void ParseNetworkMapFunction::map(KeyValuePair& kvPair,
                                KVPairWriterInterface& writer) {



  fprintf(stdout, "At node id %ld (out of %ld): num vertices %ld, URL %s \n",
                    mypeerid, numPeers, numVertices, fileURL.c_str());

  // two additional slots
  // to store <number of edges> and <page rank value>
  uint64_t maxAdjListSize = (numVertices < maxNeighbors) ?
                                             numVertices : maxNeighbors;
  uint64_t *outputValue = new uint64_t[maxAdjListSize + 2];


  uint64_t numVerticesPerPeer = numVertices / numPeers;
  if(numVertices % numPeers != 0){
    numVerticesPerPeer++;
  }
  uint64_t beginningVertex = mypeerid * numVerticesPerPeer;
  uint64_t endingVertex = beginningVertex + numVerticesPerPeer;
  if(endingVertex > numVertices){
    endingVertex = numVertices;
  }


  std::ifstream ifs(fileURL.c_str());
  ABORT_IF(!ifs.good(), "Cannot open %s\n", fileURL.c_str());
  std::string line;
  uint64_t lineno = 0;
  uint64_t numParsed = 0;
  while(getline(ifs, line)){
    if(lineno < beginningVertex){
      lineno++;
      continue;
    } else if(lineno >= endingVertex){
      break;
    } else {
      std::istringstream iss(line);
      std::vector<std::string> tokens;
      copy(std::istream_iterator<std::string>(iss),
            std::istream_iterator<std::string>(),
            std::back_inserter<std::vector<std::string> > (tokens));

      uint64_t numEdges = atol(tokens.at(1).c_str());
      ABORT_IF(numEdges != tokens.size() - 2, "Mismatch in network edge info %d,"
        "tokens %d at vtex %s\n", numEdges, tokens.size(), tokens.at(0).c_str());
      if(numEdges > maxNeighbors){
        numEdges = maxNeighbors;
      }

      //fprintf(stdout, "At vertex %ld, %ld edges: ",
      //                atol(tokens.at(0).c_str()), numEdges);

      for(uint64_t j = 0; j < numEdges; j++){
        uint64_t nid = atol(tokens.at(j+2).c_str());
        outputValue[j + 1] = sha1sum1(nid);
        //fprintf(stdout, "%ld ", nid);
      }
      //fprintf(stdout, "\n");

      double initialPageRank = 1.0 / numVertices;
      outputValue[0] = numEdges;
      memcpy(&outputValue[numEdges + 1], &initialPageRank, sizeof(double));

      KeyValuePair outputKVPair;
      uint64_t key = sha1sum1(atol(tokens.at(0).c_str()));
      outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&key),
          sizeof(uint64_t));
      outputKVPair.setValue(reinterpret_cast<const uint8_t *>(outputValue),
          (numEdges + 2) * sizeof(uint64_t));

      writer.write(outputKVPair);

      lineno++;
      numParsed++;
    }
  }

  ABORT_IF(numParsed != endingVertex - beginningVertex,
                                      "Mismatch in parsing network\n");
  delete[] outputValue;
  ifs.close();

}
