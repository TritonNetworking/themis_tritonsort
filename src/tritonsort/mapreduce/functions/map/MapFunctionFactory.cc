#include "BytesCountMapFunction.h"
#include "CombiningWordCountMapFunction.h"
#include "DiskBenchmarkMapFunction.h"
#include "GenPowerLawRandomNetworkMapFunction.h"
#include "GenRandomKMeansDataMapFunction.h"
#include "GenRandomNetworkMapFunction.h"
#include "GenRandomRatioDataMapFunction.h"
#include "GrepMapFunction.h"
#include "InvertedIndexMapFunction.h"
#include "KMeansMapFunction.h"
#include "MapFunctionFactory.h"
#include "NGramMapFunction.h"
#include "PageRankMapFunction.h"
#include "ParseNetworkMapFunction.h"
#include "PassThroughMapFunction.h"
#include "RatioMapFunction.h"
#include "TupleLengthCounterMapFunction.h"
#include "WEXLinkExtractorMapFunction.h"
#include "WEXTextExtractorMapFunction.h"
#include "WordCountMapFunction.h"
#include "ZeroKeyMapFunction.h"
#include "core/Params.h"

MapFunction* MapFunctionFactory::getNewMapFunctionInstance(
  const std::string& mapName, const Params& params) {

  MapFunction* mapFunction = NULL;

  if (mapName == "PassThroughMapFunction") {
    mapFunction = new PassThroughMapFunction();
  } else if (mapName == "BytesCountMapFunction") {
    mapFunction = new BytesCountMapFunction(
      params.get<uint64_t>("BYTES_COUNT_MAP_NUM_BYTES"));
  } else if (mapName == "InvertedIndexMapFunction") {
    mapFunction = new InvertedIndexMapFunction(
      params.get<uint64_t>("NUM_VALUE_BYTES_PER_WORD"));
  } else if (mapName == "GenRandomNetworkMapFunction") {
    mapFunction = new GenRandomNetworkMapFunction(
      params.get<uint64_t>("NUM_VERTICES"),
      params.get<double>("EDGE_PROBABILITY"),
      params.get<uint64_t>("MAX_NEIGHBORS"),
      params.get<uint64_t>("MYPEERID"),
      params.get<uint64_t>("NUM_PEERS"));
  } else if (mapName == "GenPowerLawRandomNetworkMapFunction") {
    mapFunction = new GenPowerLawRandomNetworkMapFunction(
      params.get<uint64_t>("NUM_VERTICES"),
      params.get<uint64_t>("MAX_VERTEX_DEGREE"),
      params.get<uint64_t>("MIN_VERTEX_DEGREE"),
      params.get<uint64_t>("MYPEERID"),
      params.get<uint64_t>("NUM_PEERS"));
  } else if (mapName == "ParseNetworkMapFunction") {
    mapFunction = new ParseNetworkMapFunction(
      params.get<uint64_t>("NUM_VERTICES"),
      params.get<std::string>("FILE_URL"),
      params.get<uint64_t>("MAX_NEIGHBORS"),
      params.get<uint64_t>("MYPEERID"),
      params.get<uint64_t>("NUM_PEERS"));
  } else if (mapName == "GenRandomKMeansDataMapFunction") {
    mapFunction = new GenRandomKMeansDataMapFunction(
      params.get<uint64_t>("NUM_POINTS"),
      params.get<uint64_t>("DIMENSION"),
      params.get<uint64_t>("MAX_POINT_COORD"),
      params.get<uint64_t>("KMEANS_K"),
      params.get<std::string>("KCENTERS_URL"),
      params.get<uint64_t>("MYPEERID"),
      params.get<uint64_t>("NUM_PEERS"));
  } else if (mapName == "KMeansMapFunction") {
    mapFunction = new KMeansMapFunction(
      params.get<uint64_t>("DIMENSION"),
      params.get<uint64_t>("KMEANS_K"),
      params.get<std::string>("KCENTERS_URL_IN"));
  } else if (mapName == "GenRandomRatioDataMapFunction") {
    mapFunction = new GenRandomRatioDataMapFunction(
      params.get<uint64_t>("DATA_SIZE"),
      params.get<uint64_t>("VALUE_DIMENSION"),
      params.get<uint64_t>("MYPEERID"),
      params.get<uint64_t>("NUM_PEERS"));
  } else if (mapName == "RatioMapFunction") {
    mapFunction = new RatioMapFunction(
      params.get<double>("MAP_RATIO"));
  } else if (mapName == "PageRankMapFunction") {
    mapFunction = new PageRankMapFunction();
  } else if (mapName == "WordCountMapFunction") {
    mapFunction = new WordCountMapFunction();
  } else if (mapName == "TupleLengthCounterMapFunction") {
    mapFunction = new TupleLengthCounterMapFunction();
  } else if (mapName == "GrepMapFunction") {
    mapFunction = new GrepMapFunction(params.get<double>("GREP_SELECTIVITY"));
  } else if (mapName == "DiskBenchmarkMapFunction") {
    std::string hostname;
    getHostname(hostname);
    mapFunction = new DiskBenchmarkMapFunction(
      hostname,
      params.get<uint64_t>("MAX_WRITE_SIZE"),
      params.get<uint64_t>("ALIGNMENT_MULTIPLE"),
      params.get<uint64_t>("WRITABLE_KV_PAIR_BUFFER_SIZE"),
      params.get<uint64_t>("DISK_BENCHMARK_DATA_SIZE"));
  } else if (mapName == "ZeroKeyMapFunction") {
    mapFunction = new ZeroKeyMapFunction();
  } else if (mapName == "CombiningWordCountMapFunction") {
    mapFunction = new CombiningWordCountMapFunction(
      params.get<uint64_t>("COMBINING_WORD_COUNT_MAX_COMBINER_ENTRIES"));
  } else if (mapName == "WEXLinkExtractorMapFunction") {
    mapFunction = new WEXLinkExtractorMapFunction();
  } else if (mapName == "WEXTextExtractorMapFunction") {
    mapFunction = new WEXTextExtractorMapFunction();
  } else if (mapName == "NGramMapFunction") {
    mapFunction = new NGramMapFunction(params.get<uint64_t>("NGRAM_COUNT"));
  } else {
    ABORT("Unknown map function %s", mapName.c_str());
  }

  if (mapFunction != NULL) {
    mapFunction->init(params);
  }

  return mapFunction;
}
