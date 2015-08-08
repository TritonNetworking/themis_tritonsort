#ifndef MAPRED_UTILS_H
#define MAPRED_UTILS_H

#include <string>

class Params;
class WorkerTracker;

/**
   Load read requests from the cluster coordinator into the reader's tracker.

   \param readerTracker the reader stage's tracker

   \param params the global params object

   \param phaseName the name of the phase
 */
void loadReadRequests(
  WorkerTracker& readerTracker, const Params& params,
  const std::string& phaseName);

/**
   Set the number of partitions in the coordinator for a job based on the
   intermediate to input ratio.

   \param jobID the ID of the job

   \param intermediateToInputRatio the ratio of intermediate to input data after
   the map function is applied

   \param params the global params object

   \return the number of partition
 */
uint64_t setNumPartitions(
  uint64_t jobID, double intermediateToInputRatio, const Params& params);

#endif // MAPRED_UTILS_H
