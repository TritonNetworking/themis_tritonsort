#include "core/Params.h"
#include "mapreduce/functions/reduce/ClickLogSessionSummarizerReduceFunction.h"
#include "mapreduce/functions/reduce/CountDuplicateKeysReduceFunction.h"
#include "mapreduce/functions/reduce/DiskBenchmarkReduceFunction.h"
#include "mapreduce/functions/reduce/GenPowerLawRandomNetworkReduceFunction.h"
#include "mapreduce/functions/reduce/IdentityReduceFunction.h"
#include "mapreduce/functions/reduce/InvertedIndexReduceFunction.h"
#include "mapreduce/functions/reduce/KMeansReduceFunction.h"
#include "mapreduce/functions/reduce/PageRankReduceFunction.h"
#include "mapreduce/functions/reduce/RatioReduceFunction.h"
#include "mapreduce/functions/reduce/ReduceFunctionFactory.h"
#include "mapreduce/functions/reduce/SumValuesReduceFunction.h"
#include "mapreduce/functions/reduce/WEXAdjacencyToPageRankReducer.h"
#include "mapreduce/functions/reduce/WordCountReduceFunction.h"
#include "mapreduce/functions/reduce/cloudBurst/CloudBurstReduceFunction.h"

ReduceFunction* ReduceFunctionFactory::getNewReduceFunctionInstance(
  const std::string& reduceName, const Params& params) {

  if (reduceName == "IdentityReduceFunction") {
    return new IdentityReduceFunction();
  } else if (reduceName == "SumValuesReduceFunction") {
    return new SumValuesReduceFunction();
  } else if (reduceName == "InvertedIndexReduceFunction") {
    return new InvertedIndexReduceFunction();
  } else if (reduceName == "PageRankReduceFunction"){
    return new PageRankReduceFunction();
  } else if (reduceName == "DiskBenchmarkReduceFunction") {
    return new DiskBenchmarkReduceFunction();
  } else if (reduceName == "GenPowerLawRandomNetworkReduceFunction") {
    return new GenPowerLawRandomNetworkReduceFunction(
        params.get<uint64_t>("NUM_VERTICES"),
        params.get<uint64_t>("MAX_VERTEX_DEGREE"));
  } else if (reduceName == "KMeansReduceFunction") {
    return new KMeansReduceFunction(
        params.get<uint64_t>("DIMENSION"),
        params.get<std::string>("KCENTERS_URL_OUT"));
  } else if (reduceName == "RatioReduceFunction") {
    return new RatioReduceFunction(
        params.get<double>("REDUCE_RATIO"));
  } else if (reduceName == "CloudBurstReduceFunction") {
    return new CloudBurstReduceFunction(
        params.get<uint32_t>("CLOUDBURST_MAX_ALIGN_DIFF"),
        params.get<uint32_t>("CLOUDBURST_SEED_LEN"),
        params.get<uint32_t>("CLOUDBURST_ALLOW_DIFFERENCES"),
        params.get<uint32_t>("CLOUDBURST_BLOCK_SIZE"),
        params.get<uint32_t>("CLOUDBURST_REDUNDANCY"));
  } else if (reduceName == "WEXAdjacencyToPageRankReducer") {
    return new WEXAdjacencyToPageRankReducer();
  } else if (reduceName == "ClickLogSessionSummarizerReduceFunction") {
    return new ClickLogSessionSummarizerReduceFunction(
      params.get<uint64_t>("CLICK_LOG_SUMMARIZER_SESSION_TIME_THRESHOLD"));
  } else if (reduceName == "WordCountReduceFunction") {
    return new WordCountReduceFunction();
  } else if (reduceName == "CountDuplicateKeysReduceFunction") {
    return new CountDuplicateKeysReduceFunction();
  } else {
    ABORT("Unknown reduce function %s", reduceName.c_str());
  }
  return NULL;
}
