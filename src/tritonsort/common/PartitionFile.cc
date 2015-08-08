#include <limits>
#include <re2/re2.h>

#include "common/PartitionFile.h"
#include "core/TritonSortAssert.h"

bool PartitionFile::matchFilename(
  const std::string& filename, uint64_t& partitionID, uint64_t& jobID,
  uint64_t& chunkID) {

  // Match job_JOB_whatever_PARTITIONID
  // Also match .large suffix
  static const re2::RE2 jobAndPartitionExtractionRegex(
    "job_(\\d+)/(\\d+).partition(.large)?$");

  // If not a partition file, match chunk files.
  static const re2::RE2 jobAndPartitionAndChunkExtractionRegex(
    "job_(\\d+)/(\\d+).partition.chunk_(\\d+)$");

  bool matchedRegex = re2::RE2::PartialMatch(
    filename.c_str(), jobAndPartitionExtractionRegex,
    &jobID, &partitionID);

  if (!matchedRegex) {
    // Try matching a chunk file.
    matchedRegex = re2::RE2::PartialMatch(
      filename.c_str(), jobAndPartitionAndChunkExtractionRegex,
      &jobID, &partitionID, &chunkID);
  } else {
    // Not a chunk.
    chunkID = std::numeric_limits<uint64_t>::max();
  }

  return matchedRegex;
}

PartitionFile::PartitionFile(const std::string& filename)
  : File(filename) {

  uint64_t chunkID;
  bool matchedRegex = matchFilename(filename, partitionID, jobID, chunkID);

  ABORT_IF(!matchedRegex, "Filename '%s' doesn't match the expected file "
           "naming convention for a partition file (expected "
           "'job_<job ID>/<partition ID>.partition')", filename.c_str());
}

uint64_t PartitionFile::getPartitionID() const {
  return partitionID;
}

uint64_t PartitionFile::getJobID() const {
  return jobID;
}
