#include <json/value.h>
#include <json/writer.h>
#include <sstream>

#include "core/MemoryUtils.h"
#include "core/Params.h"
#include "core/RedisCommand.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/common/RecoveryInfo.h"
#include "mapreduce/common/RedisCoordinatorClient.h"

RedisCoordinatorClient::RedisCoordinatorClient(
  const Params& params, const std::string& _phaseName, const std::string& _role,
  uint64_t _id)
  : phaseName(_phaseName),
    role(_role),
    address(params.get<std::string>("MYIPADDRESS")),
    id(_id),
    popTimeout(params.get<uint64_t>("REDIS_POP_TIMEOUT")),
    batchID(params.get<uint64_t>("BATCH_ID")),
    numNodes(params.get<uint64_t>("NUM_PEERS")),
    redisPollInterval(params.get<uint64_t>("REDIS_POLL_INTERVAL")),
    readRequestPopCommand(NULL) {

  std::list<uint64_t> jobIDList;

  parseCommaDelimitedList< uint64_t, std::list<uint64_t> >(
    jobIDList, params.get<std::string>("JOB_IDS"));

  currentBatchJobIDs.insert(jobIDList.begin(), jobIDList.end());

  std::ostringstream oss;
  oss << "read_requests:" << params.get<std::string>("MY_IP_ADDRESS") << ':'
      << role << ':' << id;

  std::string readRequestQueueName(oss.str());

  // Construct read request pop command ("BLPOP <read request queue> <timeout>")
  oss.str("");
  oss << "BLPOP " << readRequestQueueName << " " << popTimeout;

  std::string popCommandString(oss.str());
  uint64_t commandStringSize = popCommandString.size() + 1;

  readRequestPopCommand = new char[commandStringSize];
  memset(readRequestPopCommand, 0, commandStringSize);

  strncpy(readRequestPopCommand, popCommandString.c_str(), commandStringSize);
    }

RedisCoordinatorClient::~RedisCoordinatorClient() {
  if (readRequestPopCommand != NULL) {
    delete[] readRequestPopCommand;
    readRequestPopCommand = NULL;
  }
}

ReadRequest* RedisCoordinatorClient::getNextReadRequest() {
  ReadRequest* readRequest = NULL;

  bool halt = false;

  while (readRequest == NULL && !halt) {
    RedisCommand popCmd(readRequestPopCommand);
    redisReply& reply = popCmd.reply();

    readRequest = parseReadRequest(reply, halt);
  }

  return readRequest;
}

ReadRequest* RedisCoordinatorClient::parseReadRequest(
  redisReply& reply, bool& halt) {

  redisReply* replyElement = NULL;

  uint64_t replyType;
  bool parseSuccessful;
  Json::Value replyJson;
  Json::Value* jobIDs;
  std::set<uint64_t> jobIDSet;
  std::string errorString;

  switch (reply.type) {
  case REDIS_REPLY_ARRAY:
    // Expecting a two-element multi-bulk reply, where the second element is
    // the popped element

    ABORT_IF(reply.elements != 2, "Expected a multi-bulk reply with two "
             "elements from redisCommand('%s'), but got a multi-bulk reply "
             "with %llu element(s)", readRequestPopCommand, reply.elements);

    replyElement = reply.element[1];

    // We expect the second element in the reply to be a JSON-encoded read
    // request object
    RedisCommand::assertReplyType(*replyElement, REDIS_REPLY_STRING);

    parseSuccessful = jsonReader.parse(
      replyElement->str, replyElement->str + replyElement->len, replyJson);

    ABORT_IF(!parseSuccessful, "Received malformed JSON in response to "
             "redisCommand('%s')", readRequestPopCommand);

    replyType = replyJson["type"].asUInt64();

    switch (replyType) {
    case COORDINATOR_READ_REQUEST_TYPE:
      jobIDSet.clear();
      jobIDs = &(replyJson["job_ids"]);

      if (jobIDSetMatchesExpected(jobIDs, jobIDSet)) {
        return ReadRequest::fromURL(
          jobIDSet, replyJson["path"].asString(),
          replyJson["offset"].asUInt64(), replyJson["length"].asUInt64(), id);
      } else {
        return NULL;
      }

      break;
    case COORDINATOR_HALT_REQUEST_TYPE:
      // The coordinator has instructed this worker to halt
      jobIDSet.clear();
      jobIDs = &(replyJson["job_ids"]);

      if (jobIDSetMatchesExpected(jobIDs, jobIDSet)) {
        halt = true;
      } else {
        return NULL;
      }

      break;
    default:
      ABORT("Unknown response type %llu", replyType);
      break;
    }

    break;
  case REDIS_REPLY_NIL:
    // No element could be popped or timeout expired; keep trying
    break;
  default:
    ABORT("Unexpected reply type %d received from redisCommand('%s')",
          reply.type, readRequestPopCommand);
    break;
  }

  return NULL;
}

bool RedisCoordinatorClient::jobIDSetMatchesExpected(
  Json::Value* requestJobIDs, std::set<uint64_t>& requestJobIDSet) {

  for (Json::Value::iterator jobIDsIter = requestJobIDs->begin();
       jobIDsIter != requestJobIDs->end(); jobIDsIter++) {

    uint64_t jobID = (*jobIDsIter).asUInt64();

    if (currentBatchJobIDs.count(jobID) == 0) {
      // This job ID isn't in the set of job IDs we're expecting, so the
      // request is stale
      return false;
    }

    requestJobIDSet.insert(jobID);
  }

  return true;
}


JobInfo* RedisCoordinatorClient::getJobInfo(uint64_t jobID) {
  RedisCommand getCmd("HGETALL job_info:%llu", jobID);
  redisReply& reply = getCmd.reply();

  RedisCommand::assertReplyType(reply, REDIS_REPLY_ARRAY);

  redisReply* replyKeyElement = NULL;
  redisReply* replyValueElement = NULL;

  uint64_t totalInputDataBytes = 0;
  std::string mapFunction;
  std::string reduceFunction;
  std::string partitionFunction;
  std::string inputDirectory;
  std::string intermediateDirectory;
  std::string outputDirectory;

  uint64_t numPartitions = 0;

  JobInfo* jobInfo = NULL;

  ABORT_IF(reply.elements == 0, "Command '%s' failed; key does not exist or "
           "the hash is empty", getCmd.command());

  for (uint64_t i = 0; i < reply.elements; i += 2) {
    replyKeyElement = reply.element[i];
    replyValueElement = reply.element[i + 1];

    RedisCommand::assertReplyType(*replyKeyElement, REDIS_REPLY_STRING);
    RedisCommand::assertReplyType(*replyValueElement, REDIS_REPLY_STRING);

    if(strcmp(replyKeyElement->str, "job_id") == 0) {
      try {
        ASSERT(boost::lexical_cast<uint64_t>(replyValueElement->str) == jobID,
               "Job ID %x returned from command '%s' "
               "does not match expected job ID %x",
               boost::lexical_cast<uint64_t>(replyValueElement->str),
               getCmd.command(), jobID);
      } catch (boost::bad_lexical_cast& exception) {
        ABORT("Can't cast job ID '%s' to a uint64_t for comparison",
              replyValueElement->str);
      }
    } else if (strcmp(replyKeyElement->str, "total_input_size_bytes") == 0) {
      totalInputDataBytes = boost::lexical_cast<uint64_t>(
        replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "map_function") == 0) {
      mapFunction.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "reduce_function") == 0) {
      reduceFunction.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "partition_function") == 0) {
      partitionFunction.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "input_directory") == 0) {
      inputDirectory.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "intermediate_directory") == 0) {
      intermediateDirectory.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "output_directory") == 0) {
      outputDirectory.assign(replyValueElement->str);
    } else if (strcmp(replyKeyElement->str, "num_partitions") == 0) {
      try {
        numPartitions = boost::lexical_cast<uint64_t>(
          replyValueElement->str);
      } catch (boost::bad_lexical_cast& exception) {
        ABORT("Can't cast num partitions '%s' to a uint64_t",
              replyValueElement->str);
      }
    }
  }

  ASSERT(inputDirectory.length() > 0, "Input directory read from job info "
         "should have non-zero length");
  ASSERT(intermediateDirectory.length() > 0, "Intermediate directory read from "
         "job info should have non-zero length");
  ASSERT(outputDirectory.length() > 0, "Output directory read from job info "
         "should have non-zero length");
  ASSERT(mapFunction.length() > 0, "Map function read from job info "
         "should have non-zero length");
  ASSERT(reduceFunction.length() > 0, "Reduce function read from job info "
         "should have non-zero length");
  ASSERT(partitionFunction.length() > 0, "Partition function read from job "
         "info should have non-zero length");

  jobInfo = new JobInfo(
    jobID, inputDirectory, intermediateDirectory, outputDirectory, mapFunction,
    reduceFunction, partitionFunction, totalInputDataBytes, numPartitions);

  return jobInfo;
}

const URL& RedisCoordinatorClient::getOutputDirectory(
  uint64_t jobID) {
  // Cache the output directories since this function will be called multiple
  // times.
  OutputDirectoryMap::iterator iter = outputDirectories.find(jobID);
  if (iter != outputDirectories.end()) {
    return iter->second;
  } else {
    JobInfo* jobInfo = getJobInfo(jobID);

    std::string outputDirectory;

    if (phaseName == "phase_one") {
      outputDirectory.assign(jobInfo->intermediateDirectory);
    } else if (phaseName == "phase_two") {
      outputDirectory.assign(jobInfo->outputDirectory);
    } else if (phaseName == "phase_three") {
      // Chunk files are written to the intermediate directory.
      outputDirectory.assign(jobInfo->intermediateDirectory);
    } else {
      ABORT("Unsupported phase name %s", phaseName.c_str());
    }

    delete jobInfo;

    outputDirectories.insert(
      std::pair<uint64_t, URL>(jobID, URL(outputDirectory)));

    // Now grab the URL from the map.
    return outputDirectories.at(jobID);
  }
}

RecoveryInfo* RedisCoordinatorClient::getRecoveryInfo(uint64_t jobID) {
  RedisCommand existsCmd("EXISTS recovery_info:%llu", jobID);
  redisReply& existsReply = existsCmd.reply();

  RedisCommand::assertReplyType(existsReply, REDIS_REPLY_INTEGER);

  if (existsReply.integer == 0) {
    // This job isn't recovering anything, and we can safely return
    return NULL;
  }

  // Get recovery info from the recovery_info:<job ID> hash
  RedisCommand recoveryInfoCmd("HGETALL recovery_info:%llu", jobID);
  redisReply& recoveryInfoReply = recoveryInfoCmd.reply();

  RedisCommand::assertReplyType(recoveryInfoReply, REDIS_REPLY_ARRAY);

  redisReply* replyKeyElement;
  redisReply* replyValueElement;

  uint64_t recoveringJobID = 0;

  for (uint64_t i = 0; i < recoveryInfoReply.elements; i += 2) {
    replyKeyElement = recoveryInfoReply.element[i];
    replyValueElement = recoveryInfoReply.element[i + 1];

    RedisCommand::assertReplyType(*replyKeyElement, REDIS_REPLY_STRING);
    RedisCommand::assertReplyType(*replyValueElement, REDIS_REPLY_STRING);

    if (strcmp(replyKeyElement->str, "recovering_job") == 0) {
      recoveringJobID = boost::lexical_cast<uint64_t>(replyValueElement->str);
    } else {
      ABORT("Unexpected key '%s' in array returned from Redis command '%s'",
            replyKeyElement->str, recoveryInfoCmd.command());
    }
  }

  // Get partition ranges to recover from recovering_partitions:<job ID>
  ABORT_IF(recoveringJobID == 0, "Couldn't find a job ID for job %llu to "
           "recover", jobID);

  RedisCommand partitionRangesCmd(
    "SMEMBERS recovering_partitions:%llu", recoveringJobID);
  redisReply& partitionRangesReply = partitionRangesCmd.reply();

  RedisCommand::assertReplyType(partitionRangesReply, REDIS_REPLY_ARRAY);

  redisReply* replyElement;

  std::list< std::pair<uint64_t, uint64_t> > partitionRanges;

  for (uint64_t i = 0; i < partitionRangesReply.elements; i++) {
    replyElement = partitionRangesReply.element[i];

    RedisCommand::assertReplyType(*replyElement, REDIS_REPLY_STRING);

    const char* rangeStr = replyElement->str;
    uint64_t rangeStrLength = replyElement->len;

    uint64_t strIndex = 0;

    while (rangeStr[strIndex] != '-' && strIndex < rangeStrLength) {
      strIndex++;
    }

    if (strIndex == rangeStrLength) {
      std::string rangeString(rangeStr, rangeStrLength);
      ABORT_IF(strIndex == rangeStrLength, "Malformed partition range '%s'",
               rangeString.c_str());
    }

    std::string startPartitionStr(rangeStr, strIndex);
    std::string endPartitionStr(
      rangeStr + strIndex + 1, rangeStrLength - strIndex - 1);

    uint64_t startPartition = 0;
    uint64_t endPartition = 0;

    try {
      startPartition = boost::lexical_cast<uint64_t>(startPartitionStr);
    } catch (boost::bad_lexical_cast& exception) {
      ABORT("Can't cast '%s' (partition lower bound) to an integer",
            startPartitionStr.c_str());
    }

    try {
      endPartition = boost::lexical_cast<uint64_t>(endPartitionStr);
    } catch (boost::bad_lexical_cast& exception) {
      ABORT("Can't cast '%s' (partition lower bound) to an integer",
            endPartitionStr.c_str());
    }

    partitionRanges.push_back(std::make_pair(startPartition, endPartition));
  }

  RecoveryInfo* recoveryInfo = new (themis::memcheck) RecoveryInfo(
    recoveringJobID, partitionRanges);

  return recoveryInfo;
}

void RedisCoordinatorClient::notifyNodeFailure(
  const std::string& peerIPAddress) {

  notifyFailure(peerIPAddress, NULL);
}

void RedisCoordinatorClient::notifyDiskFailure(
  const std::string& peerIPAddress, const std::string& diskPath) {

  notifyFailure(peerIPAddress, &diskPath);
}

void RedisCoordinatorClient::setNumPartitions(
  uint64_t jobID, uint64_t numPartitions) {
  RedisCommand updateNumPartitions(
    "HSET job_info:%llu num_partitions %llu", jobID, numPartitions);
}

void RedisCoordinatorClient::notifyFailure(
  const std::string& peerIPAddress, const std::string* diskPath) {

  // Translate IP address into hostname
  RedisCommand ipToHostCmd("HGET hostname %s", peerIPAddress.c_str());
  redisReply& reply = ipToHostCmd.reply();

  RedisCommand::assertReplyType(reply, REDIS_REPLY_STRING);

  std::string hostname(reply.str, reply.len);

  Json::Value failureReport;

  failureReport["hostname"] = hostname;
  failureReport["batch_id"] = Json::UInt64(batchID);

  if (diskPath != NULL) {
    failureReport["disk"] = *diskPath;
  } else {
    failureReport["disk"] = Json::Value();
  }

  failureReport["message"] = "internal_report";

  Json::FastWriter writer;

  std::string failureReportString(writer.write(failureReport));

  RedisCommand reportFailureCmd(
    "RPUSH node_failure_reports %s", failureReportString.c_str());
}

void RedisCoordinatorClient::waitOnBarrier(const std::string& barrierName) {
  uint64_t jobID = 0;
  if (phaseName.compare("phase_zero") == 0 ||
      phaseName.compare("phase_three") == 0) {
    // Phase zero and three requires a barrier per job.
    ASSERT(currentBatchJobIDs.size() == 1,
           "Somehow got a weird number of jobs in phase zero or three. "
           "Expected 1, got %llu", currentBatchJobIDs.size());
    jobID = *(currentBatchJobIDs.begin());
  }

  // When we reach the barrier we remove ourselves from the barrier set.
  RedisCommand reachBarrier(
    "SREM barrier:%s:%s:%llu:%llu %s", barrierName.c_str(), phaseName.c_str(),
    batchID, jobID, address.c_str());

  redisReply& reachBarrierReply = reachBarrier.reply();
  RedisCommand::assertReplyType(reachBarrierReply, REDIS_REPLY_INTEGER);

  ASSERT(reachBarrierReply.integer == 1, "Command failed with error %llu: %s",
         reachBarrierReply.integer, reachBarrier.command());

  while (true) {
    // Check to see if the barrier still holds.
    RedisCommand checkBarrier(
      "EXISTS barrier:%s:%s:%llu:%llu", barrierName.c_str(),
      phaseName.c_str(), batchID, jobID);

    redisReply& checkBarrierReply = checkBarrier.reply();
    RedisCommand::assertReplyType(checkBarrierReply, REDIS_REPLY_INTEGER);

    if (checkBarrierReply.integer == 0) {
      // Barrier set no longer exists, which means all nodes have cleared it.
      break;
    }

    usleep(redisPollInterval);
  }
}

void RedisCoordinatorClient::uploadSampleStatistics(
  uint64_t jobID, uint64_t inputBytes, uint64_t intermediateBytes) {
  RedisCommand updateInputBytes(
    "RPUSH input_bytes:%llu %llu", jobID, inputBytes);
  RedisCommand updateIntermediateBytes(
    "RPUSH intermediate_bytes:%llu %llu", jobID, intermediateBytes);
}

void RedisCoordinatorClient::getSampleStatisticsSums(
  uint64_t jobID, uint64_t numNodes, uint64_t& totalInputBytes,
  uint64_t& totalIntermediateBytes) {

  // Wait until all nodes have uploaded sample statistics.
  while (true) {
    RedisCommand checkInputBytes(
      "LLEN input_bytes:%llu", jobID);

    redisReply& checkReply = checkInputBytes.reply();
    RedisCommand::assertReplyType(checkReply, REDIS_REPLY_INTEGER);

    if (static_cast<uint64_t>(checkReply.integer) == numNodes) {
      // We have all input byte stats.
      break;
    }

    usleep(redisPollInterval);
  }

  while (true) {
    RedisCommand checkIntermediateBytes(
      "LLEN intermediate_bytes:%llu", jobID);

    redisReply& checkReply = checkIntermediateBytes.reply();
    RedisCommand::assertReplyType(checkReply, REDIS_REPLY_INTEGER);

    if (static_cast<uint64_t>(checkReply.integer) == numNodes) {
      // We have all intermediate byte stats.
      break;
    }

    usleep(redisPollInterval);
  }

  totalInputBytes = 0;
  totalIntermediateBytes = 0;
  for (uint64_t i = 0; i < numNodes; i++) {
    RedisCommand getInputBytes(
      "LPOP input_bytes:%llu", jobID);

    redisReply& getInput = getInputBytes.reply();
    RedisCommand::assertReplyType(getInput, REDIS_REPLY_STRING);

    totalInputBytes += boost::lexical_cast<uint64_t>(getInput.str);

    RedisCommand getIntermediateBytes(
      "LPOP intermediate_bytes:%llu", jobID);

    redisReply& getIntermediate = getIntermediateBytes.reply();
    RedisCommand::assertReplyType(getIntermediate, REDIS_REPLY_STRING);

    totalIntermediateBytes +=
      boost::lexical_cast<uint64_t>(getIntermediate.str);
  }
}

uint64_t RedisCoordinatorClient::getNumPartitions(uint64_t jobID) {
  while (true) {
    RedisCommand getNumPartitions(
      "HGET job_info:%llu num_partitions", jobID);

    redisReply& getReply = getNumPartitions.reply();
    if (getReply.type == REDIS_REPLY_STRING) {
      return boost::lexical_cast<uint64_t>(getReply.str);
    }

    // We're still waiting for the coordinator to set it.
    RedisCommand::assertReplyType(getReply, REDIS_REPLY_NIL);

    usleep(redisPollInterval);
  }
}
