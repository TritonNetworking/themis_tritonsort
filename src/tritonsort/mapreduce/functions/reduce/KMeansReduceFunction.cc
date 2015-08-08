#include <arpa/inet.h>
#include <iostream>
#include <fstream>

#include "KMeansReduceFunction.h"
#include "mapreduce/common/KeyValuePair.h"

KMeansReduceFunction::KMeansReduceFunction(uint64_t _dimension,
                                     std::string _kCentersURL)
  : dimension(_dimension), kCentersURL(_kCentersURL){
}

void KMeansReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  uint64_t *centerVector = new uint64_t[dimension];
  for(uint64_t j = 0; j < dimension; j++){
    centerVector[j] = 0;
  }

  uint64_t numTuples = 0;

  while (iterator.next(kvPair)) {
    uint64_t *v = (uint64_t *)kvPair.getValue();
    for(uint64_t j = 0; j < dimension; j++){
      centerVector[j] += v[j];
    }

    numTuples++;
  }

  for(uint64_t j = 0; j < dimension; j++){
    centerVector[j] /= numTuples;
  }

  KeyValuePair outputKVPair;
  outputKVPair.setKey(key, keyLength);
  outputKVPair.setValue(reinterpret_cast<uint8_t *>(centerVector),
                                    dimension * sizeof(uint64_t));
  writer.write(outputKVPair);

  // write the params to the NFS
  uint64_t *pCenterID = (uint64_t *)key;
  std::ofstream outfile;
  outfile.open (kCentersURL.c_str(), std::ios::out | std::ios::app);
  ABORT_IF(!outfile.is_open(), "Cannot open file %s\n", kCentersURL.c_str());

  outfile << (*pCenterID & 0xFFFFFFFFL) << " " << *pCenterID;
  for(uint64_t j = 0; j < dimension; j++){
    outfile << " " << centerVector[j];
  }
  outfile << std::endl;
  outfile.close();

  delete[] centerVector;
}
