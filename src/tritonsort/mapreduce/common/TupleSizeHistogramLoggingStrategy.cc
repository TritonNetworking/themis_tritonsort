#include <boost/algorithm/string.hpp>

#include "TupleSizeHistogramLoggingStrategy.h"

TupleSizeHistogramLoggingStrategy::TupleSizeHistogramLoggingStrategy(
  const std::string& _statNamePrefix,
  const Params& params, bool _reading)
  : statNamePrefix(_statNamePrefix),
    keySizeHistogramBinSize(
      params.get<uint64_t>(
        boost::to_upper_copy(_statNamePrefix)
        + "_KEY_SIZE_HISTOGRAM_BIN_SIZE")),
    valueSizeHistogramBinSize(params.get<uint64_t>(
      boost::to_upper_copy(_statNamePrefix)
      + "_VALUE_SIZE_HISTOGRAM_BIN_SIZE")),
    tupleSizeHistogramBinSize(params.get<uint64_t>(
      boost::to_upper_copy(_statNamePrefix)
      + "_TUPLE_SIZE_HISTOGRAM_BIN_SIZE")),
    reading(_reading) {
}

void TupleSizeHistogramLoggingStrategy::registerStats(StatLogger& logger) {
  keySizeStatID = logger.registerHistogramStat(statNamePrefix + "_key_size",
                                               keySizeHistogramBinSize);
  valueSizeStatID = logger.registerHistogramStat(statNamePrefix + "_value_size",
                                                 valueSizeHistogramBinSize);
  tupleSizeStatID = logger.registerHistogramStat(statNamePrefix + "_tuple_size",
                                                 tupleSizeHistogramBinSize);
}

void TupleSizeHistogramLoggingStrategy::logTuple(StatLogger& logger,
                                                 KeyValuePair& kvPair) {
  logger.add(keySizeStatID, kvPair.getKeyLength());
  logger.add(valueSizeStatID, kvPair.getValueLength());
  if (reading) {
    logger.add(tupleSizeStatID, kvPair.getReadSize());
  } else {
    logger.add(tupleSizeStatID, kvPair.getWriteSize());
  }
}
