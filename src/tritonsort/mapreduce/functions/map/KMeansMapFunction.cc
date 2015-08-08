#include <iterator>

#include "KMeansMapFunction.h"

KMeansMapFunction::KMeansMapFunction(uint64_t _dimension, uint64_t _k,
                                     std::string _kCentersURL)
  : dimension(_dimension), k(_k), kCentersURL(_kCentersURL) {
}

void KMeansMapFunction::init(const Params& params) {
  kCenters = new uint64_t*[k];
  for(uint64_t i = 0; i < k; i++){
    // additional one slot to store hashed center id
    kCenters[i] = new uint64_t[dimension + 1];
  }

  std::ifstream ifs(kCentersURL.c_str());
  ABORT_IF(!ifs.good(), "Cannot open %s\n", kCentersURL.c_str());
  std::string line;
  uint64_t lineno = 0;
  while(getline(ifs, line)){
    ASSERT(lineno < k, "Invalid k-centers param file");
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    copy(std::istream_iterator<std::string>(iss),
        std::istream_iterator<std::string>(),
        std::back_inserter<std::vector<std::string> > (tokens));

    ASSERT(dimension == tokens.size() - 2, "Mismatch in k-centers param file");
    kCenters[lineno][0] = atol(tokens.at(1).c_str());
    for(uint64_t i = 0; i < dimension; i++){
      kCenters[lineno][i + 1] = atol(tokens.at(i + 2).c_str());
    }
    lineno++;
  }
}

void KMeansMapFunction::teardown(KVPairWriterInterface& writer) {
  for(uint64_t i = 0; i < k; i++){
    delete[] kCenters[i];
  }
  delete[] kCenters;
}

void KMeansMapFunction::map(KeyValuePair& kvPair,
                            KVPairWriterInterface& writer) {
  uint64_t *coordVector = (uint64_t *)kvPair.getValue();
  uint64_t minSquaredDistance = 0;
  uint64_t minCenterID = 0;
  // find nearest center
  for(uint64_t i = 0; i < k; i++){
    uint64_t squaredDistance = 0;
    for(uint64_t j = 0; j < dimension; j++){
      int64_t dist = kCenters[i][j + 1] - coordVector[j];
      squaredDistance += dist * dist;
    }
    if(i == 0 || squaredDistance < minSquaredDistance){
      minSquaredDistance = squaredDistance;
      minCenterID = kCenters[i][0];
    }
  }

  // emit nearest center
  KeyValuePair outputKVPair;
  outputKVPair.setKey(reinterpret_cast<const uint8_t *>(&minCenterID),
      sizeof(uint64_t));
  outputKVPair.setValue(reinterpret_cast<const uint8_t *>(kvPair.getValue()),
      kvPair.getValueLength());

  writer.write(outputKVPair);
}
