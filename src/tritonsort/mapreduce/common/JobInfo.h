#ifndef THEMIS_MAPRED_JOB_INFO_H
#define THEMIS_MAPRED_JOB_INFO_H

#include <stdint.h>
#include <string>

/**
   JobInfo provides information about a particular Themis job.
 */
class JobInfo {
public:
  /// Constructor
  /**
     \param jobID the unique job ID of this job

     \param inputDirectory the directory from which the job's input is read

     \param intermediateDirectory the directory from which the job's
     intermediate data is written

     \param outputDirectory the directory to which the job's output is written

     \param mapFunction the name of the job's map function

     \param reduceFunction the name of the job's reduce function

     \param partitionFunction the name of the job's partitioning function

     \param totalInputSize the total size of the job's input in bytes

     \param numPartitions the number of partitions this job will create
   */
  JobInfo(
    uint64_t jobID, const std::string& inputDirectory,
    const std::string& intermediateDirectory,
    const std::string& outputDirectory, const std::string& mapFunction,
    const std::string& reduceFunction, const std::string& partitionFunction,
    uint64_t totalInputSize, uint64_t numPartitions);

  const uint64_t jobID;
  const std::string inputDirectory;
  const std::string intermediateDirectory;
  const std::string outputDirectory;
  const std::string mapFunction;
  const std::string reduceFunction;
  const std::string partitionFunction;
  const uint64_t totalInputSize;
  const uint64_t numPartitions;
};


#endif // THEMIS_MAPRED_JOB_INFO_H
