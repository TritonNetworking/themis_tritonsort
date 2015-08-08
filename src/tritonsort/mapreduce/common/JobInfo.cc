#include "mapreduce/common/JobInfo.h"

JobInfo::JobInfo(
  uint64_t _jobID, const std::string& _inputDirectory,
  const std::string& _intermediateDirectory,
  const std::string& _outputDirectory, const std::string& _mapFunction,
  const std::string& _reduceFunction, const std::string& _partitionFunction,
  uint64_t _totalInputSize, uint64_t _numPartitions)
  : jobID(_jobID),
    inputDirectory(_inputDirectory),
    intermediateDirectory(_intermediateDirectory),
    outputDirectory(_outputDirectory),
    mapFunction(_mapFunction),
    reduceFunction(_reduceFunction),
    partitionFunction(_partitionFunction),
    totalInputSize(_totalInputSize),
    numPartitions(_numPartitions) {
}

