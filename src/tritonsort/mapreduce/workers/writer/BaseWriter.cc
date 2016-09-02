#include <boost/filesystem.hpp>
#include <sstream>
#include <iomanip>

#include "common/MainUtils.h"
#include "common/WriteTokenPool.h"
#include "core/File.h"
#include "core/MemoryUtils.h"
#include "core/NamedObjectCollection.h"
#include "core/Params.h"
#include "core/StatLogger.h"
#include "mapreduce/common/ChunkMap.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/URL.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/writer/BaseWriter.h"

BaseWriter* BaseWriter::newBaseWriter(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies,
  File::AccessMode fileMode, StatLogger& logger) {

  std::string coordinatorPhaseName = phaseName;
  if (params.contains("FORCE_PHASE_TWO_OUTPUT_DIR") &&
      params.get<bool>("FORCE_PHASE_TWO_OUTPUT_DIR")) {
    // The second part of phase three needs to write to phase two output
    // directories.
    coordinatorPhaseName = "phase_two";
  }
  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(
      params, coordinatorPhaseName, stageName, id);

  // If this worker was spawned by a MultiProtocolWriter, want to make sure it
  // gets configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  bool directIO =
    params.get<bool>("DIRECT_IO." + phaseName + "." + parentStageName);

  std::string nodeIPAddress = params.get<std::string>("MYIPADDRESS");
  uint64_t logicalDiskSizeHint = 0;

  if (params.get<bool>("FILE_PREALLOCATION")) {
    // Preallocate to the partition size unless the user specifies a custom
    // preallocation size.
    if (params.contains("FILE_PREALLOCATION_SIZE")) {
      logicalDiskSizeHint = params.get<uint64_t>("FILE_PREALLOCATION_SIZE");
    } else {
      logicalDiskSizeHint = params.get<uint64_t>("PARTITION_SIZE");
    }
  }

  // Compute which disks belong to this writer.
  StringList* diskList = dependencies.get<StringList>("output_disk_list");
  uint64_t disksPerWriter = params.getv<uint64_t>(
    "DISKS_PER_WORKER.%s.%s", phaseName.c_str(), parentStageName.c_str());
  OutputDiskMap outputDisks;
  StringList::iterator iter = diskList->begin();
  for (uint64_t diskID = 0; iter != diskList->end(); diskID++, iter++) {
    // Populate output disk map with exactly the disks for this node
    if (diskID >= disksPerWriter * id && outputDisks.size() < disksPerWriter) {
      outputDisks[diskID] = *iter;
    }
  }

  TRITONSORT_ASSERT(outputDisks.size() == disksPerWriter,
         "Writer %llu should have %llu disks but has %llu", id, disksPerWriter,
         outputDisks.size());

  uint64_t bytesBeforeSimulatedFailure = 0;

  uint64_t peerID = params.get<uint64_t>("MYPEERID");
  if (params.containsv("SIMULATE_DISK_FAILURE.%llu.%llu", peerID, id)) {
    bytesBeforeSimulatedFailure = params.getv<uint64_t>(
      "SIMULATE_DISK_FAILURE.%llu.%llu", peerID, id);
  }

  WriteTokenPool* writeTokenPool = NULL;

  if (dependencies.contains<WriteTokenPool>("write_token_pool")) {
    writeTokenPool = dependencies.get<WriteTokenPool>("write_token_pool");
  }

  uint64_t numDisks =
    params.getv<uint64_t>("NUM_OUTPUT_DISKS.%s", phaseName.c_str());

  uint64_t largePartitionThreshold = 0;
  if (phaseName == "phase_one") {
    // Rename large partitions in phase one so we can process them in phase
    // three.
    largePartitionThreshold = params.get<uint64_t>("LARGE_PARTITION_THRESHOLD");
  }

  ChunkMap* chunkMap = NULL;
  if (dependencies.contains<ChunkMap>("chunk_map")) {
    chunkMap = dependencies.get<ChunkMap>("chunk_map");
  }

  BaseWriter* writer = new BaseWriter(
    id, nodeIPAddress, writeTokenPool, fileMode, directIO, logicalDiskSizeHint,
    outputDisks, *coordinatorClient, bytesBeforeSimulatedFailure, logger,
    params, numDisks, phaseName, largePartitionThreshold, chunkMap,
    peerID);

  return writer;
}

BaseWriter::BaseWriter(
  uint64_t _id, const std::string& _nodeIPAddress,
  WriteTokenPool* _writeTokenPool, File::AccessMode _fileMode, bool _directIO,
  uint64_t _logicalDiskSizeHint, OutputDiskMap& _outputDisks,
  CoordinatorClientInterface& _coordinatorClient,
  uint64_t _bytesBeforeSimulatedFailure, StatLogger& _logger,
  const Params& params, uint64_t _numDisks, const std::string& phaseName,
  uint64_t _largePartitionThreshold, ChunkMap* _chunkMap, uint64_t _nodeID)
  : id(_id),
    nodeIPAddress(_nodeIPAddress),
    fileMode(_fileMode),
    directIO(_directIO),
    logicalDiskSizeHint(_logicalDiskSizeHint),
    outputDisks(_outputDisks),
    bytesBeforeSimulatedFailure(_bytesBeforeSimulatedFailure),
    numDisks(_numDisks),
    largePartitionThreshold(_largePartitionThreshold),
    nodeID(_nodeID),
    coordinatorClient(_coordinatorClient),
    partitionMap(params, phaseName),
    writeTokenPool(_writeTokenPool),
    chunkMap(_chunkMap),
    logger(_logger),
    totalBytesWritten(0) {
  writeSizeStatID = logger.registerHistogramStat("write_size", 100);
}

BaseWriter::~BaseWriter() {
  delete &coordinatorClient;
}

File* BaseWriter::getFile(KVPairBuffer* writeBuffer) {
  // Get the job ID for this write.
  const std::set<uint64_t>& jobIDs = writeBuffer->getJobIDs();
  TRITONSORT_ASSERT(jobIDs.size() == 1, "Expected buffer being passed to writer to "
         "have exactly one job ID; this one has %llu", jobIDs.size());
  uint64_t jobID = *(jobIDs.begin());

  uint64_t partitionsPerDisk =
    partitionMap.getNumPartitionsPerOutputDisk(jobID);
  uint64_t partitionID = writeBuffer->getLogicalDiskID();
  uint64_t chunkID = writeBuffer->getChunkID();
  bool replica = false;

  // Determine which disk in the cluster this partition belongs to.
  uint64_t diskID = 0;
  if (chunkID < std::numeric_limits<uint64_t>::max()) {
    // This is a chunk buffer for a large partition in phase three.
    TRITONSORT_ASSERT(chunkMap != NULL, "Require chunk map for phase three.");
    diskID = chunkMap->getDiskID(partitionID, chunkID);
  } else {
    // This is a regular partition file.
    diskID = partitionID / partitionsPerDisk;

    // Adjust disk ID to be local to this node.
    uint64_t diskNode = diskID / numDisks;
    diskID = diskID % numDisks;

    if (diskNode != nodeID) {
      // This partition belongs to a different node, so assume that it's a
      // replica partition.
      replica = true;
    }
  }

  if (blackHoleWrites[diskID]) {
    return NULL;
  }

  // Verify that this writer handles the disk.
  OutputDiskMap::const_iterator outputDiskIter = outputDisks.find(diskID);
  TRITONSORT_ASSERT(outputDiskIter != outputDisks.end(),
         "Writer %llu got buffer for logical disk %llu, physical disk %llu, "
         "but is not responsible for this physical disk.", id, partitionID,
         diskID);

  // Get the corresponding file.
  if (chunkID < std::numeric_limits<uint64_t>::max()) {
    // Excuse the notation abuse...
    FilesForJobMap& chunkFilesForJob = chunkFiles[jobID];
    FileMap& chunkFileMap = chunkFilesForJob[partitionID];
    File*& chunkFile = chunkFileMap[chunkID];

    if (chunkFile == NULL) {
      chunkFile = newFile(
        partitionID, jobID, outputDiskIter->second, chunkID, replica);
    }

    return chunkFile;
  } else {
    FileMap& filesForJob = files[jobID];
    File*& file = filesForJob[partitionID];

    if (file == NULL) {
      file = newFile(
        partitionID, jobID, outputDiskIter->second, chunkID, replica);

      // Log an example file path for the first partition on each disk
      if (partitionID % partitionsPerDisk == 0) {
        logger.logDatum("example_output_filename", file->getFilename());
      }
    }

    return file;
  }
}

void BaseWriter::logBufferWritten(KVPairBuffer* writeBuffer) {
  totalBytesWritten += writeBuffer->getCurrentSize();
  logger.add(writeSizeStatID, writeBuffer->getCurrentSize());

  if (bytesBeforeSimulatedFailure != 0 &&
      totalBytesWritten >= bytesBeforeSimulatedFailure) {
    // We did all the error checking in getFile(), so no need to repeat it here.
    const std::set<uint64_t>& jobIDs = writeBuffer->getJobIDs();
    uint64_t jobID = *(jobIDs.begin());

    // Determine local disk ID for this buffer.
    uint64_t diskID = (writeBuffer->getLogicalDiskID() /
                       partitionMap.getNumPartitionsPerOutputDisk(jobID)) %
      numDisks;

    const std::string& outputDisk = outputDisks.at(diskID);

    // Tell the coordinator that this disk failed and stop handling writes.
    coordinatorClient.notifyDiskFailure(nodeIPAddress, outputDisk);
    blackHoleWrites[diskID] = true;
  }

  // Return write token if it exists.
  WriteToken* token = writeBuffer->getToken();
  if (token != NULL) {
    ABORT_IF(writeTokenPool == NULL, "You must have provided a write token "
             "pool, but this writer hasn't been given a write token pool");
    writeTokenPool->putToken(token);
  }
}

void BaseWriter::teardown() {
  uint64_t alignedBytesWritten = 0;

  // Close all partition files
  for (FilesForJobMap::iterator jobIter = files.begin();
       jobIter != files.end(); jobIter++) {
    FileMap& filesForJob = jobIter->second;

    for (FileMap::iterator fileIter = filesForJob.begin();
         fileIter != filesForJob.end(); fileIter++) {
      File*& file = fileIter->second;

      if (file != NULL) {
        alignedBytesWritten += file->getAlignedBytesWritten();

        file->close();

        // Rename large partition files.
        if (largePartitionThreshold > 0) {
          uint64_t fileSize = file->getCurrentSize();
          if (fileSize > largePartitionThreshold) {
            file->rename(file->getFilename() + ".large");
          }
        }

        delete file;
        file = NULL;
      }
    }
  }

  files.clear();

  // Close all chunk files.
  for (ChunkFilesForJobMap::iterator jobIter = chunkFiles.begin();
       jobIter != chunkFiles.end(); jobIter++) {
    // Excuse the notation abuse...
    FilesForJobMap& filesForJob = jobIter->second;

    for (FilesForJobMap::iterator partitionIter = filesForJob.begin();
         partitionIter != filesForJob.end(); partitionIter++) {
      FileMap& chunkFileMap = partitionIter->second;

      for (FileMap::iterator fileIter = chunkFileMap.begin();
           fileIter != chunkFileMap.end(); fileIter++) {
        File*& file = fileIter->second;

        if (file != NULL) {
          alignedBytesWritten += file->getAlignedBytesWritten();

          file->close();

          delete file;
          file = NULL;
        }
      }
    }
  }

  chunkFiles.clear();

  logger.logDatum("total_bytes_written", totalBytesWritten);
  logger.logDatum("direct_io_bytes_written", alignedBytesWritten);
}

File* BaseWriter::newFile(
  uint64_t logicalDiskUID, uint64_t jobID, const std::string& outputDisk,
  uint64_t chunkID, bool replica) {

  const themis::URL& outputDirectory = coordinatorClient.getOutputDirectory(jobID);

  std::ostringstream oss;
  oss << outputDisk << outputDirectory.path();

  if (replica) {
    // Write replica files to a separate directory.
    oss << "_replica";
  }

  // Create the directory if it doesn't exist.
  if (!boost::filesystem::exists(oss.str())) {
    ABORT_IF(!boost::filesystem::create_directories(oss.str()),
             "Could not create directory %s", oss.str().c_str());
  }

  oss << "/" << std::setfill('0') << std::setw(8) << logicalDiskUID
      << ".partition";
  if (chunkID < std::numeric_limits<uint64_t>::max()) {
    // This is a large chunk buffer.
    oss << ".chunk_" << std::setfill('0') << std::setw(8) << chunkID;
  }

  // Create the file and open it for writing
  File* logicalDiskFile = new (themis::memcheck) File(oss.str());
  TRITONSORT_ASSERT(fileMode == File::WRITE || fileMode == File::WRITE_POSIXAIO ||
         fileMode == File::WRITE_LIBAIO, "Unsupported write mode, must be "
         "WRITE, WRITE_POSIXAIO, or WRITE_LIBAIO");
  logicalDiskFile->open(fileMode, true);

  if (directIO) {
    // Write with O_DIRECT
    logicalDiskFile->enableDirectIO();
  }

  // Pre-allocate the file if we are given a hint as to how big it will be.
  if (logicalDiskSizeHint > 0) {
    logicalDiskFile->preallocate(logicalDiskSizeHint);
  }

  return logicalDiskFile;
}
