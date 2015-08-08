#ifndef MAPRED_TUPLE_SIZE_HISTOGRAM_LOGGING_STRATEGY_H
#define MAPRED_TUPLE_SIZE_HISTOGRAM_LOGGING_STRATEGY_H

#include "core/Params.h"
#include "mapreduce/common/TupleSizeLoggingStrategyInterface.h"

/**
   A tuple size logging strategy that logs histograms of key, value and tuple
   size
 */
class TupleSizeHistogramLoggingStrategy
  : public TupleSizeLoggingStrategyInterface {
public:
  /// Constructor
  /**
     Bin sizes will be determined by the values of parameters
     <statNamePrefix.to_upper()>_{KEY,VALUE,TUPLE}_SIZE_HISTOGRAM_BIN_SIZE

     \param statNamePrefix stats logged for tuples will be named
     "<statNamePrefix>_key_size", "<statNamePrefix>_value_size" and
     "<statNamePrefix>_tuple_size"

     \param params a Params object from which the strategy will retrieve its
     bin sizes

     \param reading if true, log the size of tuples being read, otherwise log
     the size of tuples being written
   */
  TupleSizeHistogramLoggingStrategy(
    const std::string& statNamePrefix, const Params& params, bool reading);

  /// Registers key, value and tuple size histograms with the logger
  void registerStats(StatLogger& logger);

  /// Logs key, value and tuple size to the appropriate histograms
  void logTuple(StatLogger& logger, KeyValuePair& kvPair);

private:
  const std::string statNamePrefix;
  const uint64_t keySizeHistogramBinSize;
  const uint64_t valueSizeHistogramBinSize;
  const uint64_t tupleSizeHistogramBinSize;
  const bool reading;

  uint64_t keySizeStatID;
  uint64_t valueSizeStatID;
  uint64_t tupleSizeStatID;
};

#endif // MAPRED_TUPLE_SIZE_HISTOGRAM_LOGGING_STRATEGY_H
