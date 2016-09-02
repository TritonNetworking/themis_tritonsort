#include <sstream>

#include "common/PartitionFile.h"
#include "common/WriteToken.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/workers/reader/AsynchronousReader.h"

AsynchronousReader::AsynchronousReader(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  uint64_t _asynchronousIODepth, uint64_t defaultBufferSize,
  uint64_t alignmentSize, FilenameToStreamIDMap* _filenameToStreamIDMap,
  MemoryAllocatorInterface& memoryAllocator, bool _deleteAfterRead,
  bool _useByteStreamBuffers, WriteTokenPool* _tokenPool, ChunkMap* chunkMap)
  : MultiQueueRunnable(id, stageName),
    asynchronousIODepth(_asynchronousIODepth),
    logger(stageName, id),
    readSizeStatID(
      logger.registerHistogramStat("read_size", 100)),
    asynchronousIODepthStatID(
      logger.registerHistogramStat("asynchronous_io_depth", 4)),
    deleteAfterRead(_deleteAfterRead),
    useByteStreamBuffers(_useByteStreamBuffers),
    setStreamSize(phaseName == "phase_two"),
    alignedBytesRead(0),
    filenameToStreamIDMap(_filenameToStreamIDMap),
    byteStreamBufferFactory(
      *this, memoryAllocator, defaultBufferSize, alignmentSize),
    kvPairBufferFactory(*this, memoryAllocator, 0, alignmentSize),
    tokenPool(_tokenPool) {
  ABORT_IF(useByteStreamBuffers && filenameToStreamIDMap == NULL,
           "Filename-to-stream-ID map cannot be NULL");

  if (tokenPool != NULL) {
    ABORT_IF(chunkMap == NULL,
             "Token pools require chunk maps in phase three.");
    const ChunkMap::DiskMap& diskMap = chunkMap->getDiskMap();

    // Build the offset map.
    uint64_t currentOffset = 0;
    for (ChunkMap::DiskMap::const_iterator iter = diskMap.begin();
         iter != diskMap.end(); iter++) {
      uint64_t partitionID = iter->first;
      uint64_t numChunks = (iter->second).size();

      offsetMap[partitionID] = currentOffset;
      currentOffset += numChunks;
    }
  }
}

void AsynchronousReader::run() {
  bool done = false;
  while (!done) {
    logger.add(asynchronousIODepthStatID, numReadsInProgress());

    ReadRequest* readRequest = NULL;
    bool gotNewWork = attemptGetNewWork(getID(), readRequest);

    if (gotNewWork) {
      if (readRequest == NULL) {
        done = true;

        // Finish reading all existing requests so we don't have an artificially
        // long teardown time.
        while (!waitingForToken.empty() || numReadsInProgress() > 0) {
          if (!waitingForToken.empty()) {
            checkForReadTokens();
          }
          if (numReadsInProgress() > 0) {
            waitForReadCompletionAndEmitBuffers();
          }
        }
      } else {
        // Wait until we have a free I/O slot, and then service a new request.
        while (numReadsInProgress() >= asynchronousIODepth) {
          // The maximum number of I/Os are in flight, so wait until at least
          // one completes in the hope that it frees up a slot.
          waitForReadCompletionAndEmitBuffers();
        }

        ReadInfo* readInfo;
        if (processNewReadRequest(readRequest, readInfo)) {
          // Start reading from the beginning of this read request.
          startNextReadBuffer(readInfo);
        }
      }
    } else {
      // No new reads to issue, so process our existing requests until it's time
      // to go back to the tracker.
      Timer processTimer;
      processTimer.start();
      while (processTimer.getRunningTime() < 10000) {
        if (numReadsInProgress() == 0 && waitingForToken.empty()) {
          // There's no work, and no buffers to read, so sleep.
          uint64_t elapsed = processTimer.getRunningTime();
          if (elapsed < 10000) {
            // Sleep a little extra to make sure we don't have to do this twice.
            startWaitForWorkTimer();
            usleep(10100 - elapsed);
            stopWaitForWorkTimer();
          }
        } else {
          if (!waitingForToken.empty()) {
            // Try to issue new reads.
            checkForReadTokens();
          }
          if (numReadsInProgress() > 0) {
            // Wait until some read completes.
            waitForReadCompletionAndEmitBuffers();
          }
        }
      }
    }
  }
}

void AsynchronousReader::teardown() {
  TRITONSORT_ASSERT(numReadsInProgress() == 0,
         "Should have finished all writes before tearing down.");
  logger.logDatum("direct_io_bytes_read", alignedBytesRead);
}

bool AsynchronousReader::processNewReadRequest(
  ReadRequest* readRequest, ReadInfo*& readInfo) {
  File* file = new (themis::memcheck) File(readRequest->path);
  if (file->getCurrentSize() > 0) {
    std::ostringstream oss;
    oss << file->getFilename() << ",offset=" << readRequest->offset
        << ",length=" << readRequest->length;

    logger.logDatum("input_filename", oss.str());

    // Prepare the file for reading.
    openFile(*file);
    file->seek(readRequest->offset, File::FROM_BEGINNING);

    // Update data structures.
    bytesRead[readRequest] = 0;
    if (useByteStreamBuffers) {
      TRITONSORT_ASSERT(readRequest->jobIDs.size() > 0, "Expected read request to have "
             "at least one job ID");

      if (setStreamSize) {
        filenameToStreamIDMap->addFilenameWithSize(
          file->getFilename(), readRequest->jobIDs, readRequest->length,
          readRequest->offset);
      } else {
        filenameToStreamIDMap->addFilename(
          file->getFilename(), readRequest->jobIDs, readRequest->offset);
      }
    }

    uint64_t tokenID = 0;
    if (tokenPool != NULL) {
      // If we're using token pools, then compute the token ID for this file
      // based on the partition chunk offset and chunk ID.
      uint64_t partitionID;
      uint64_t jobID;
      uint64_t chunkID;
      bool matched = PartitionFile::matchFilename(
        file->getFilename(), partitionID, jobID, chunkID);
      ABORT_IF(!matched, "Read tokens require partition files as input.");

      tokenID = offsetMap.at(partitionID) + chunkID;
    }

    // Create a new readInfo object that will track this read.
    readInfo = new (themis::memcheck) ReadInfo(
      NULL, file, readRequest, 0, tokenID);

    return true;
  } else {
    // Don't process empty files.
    delete file;
    return false;
  }
}

BaseBuffer* AsynchronousReader::startNextReadBuffer(
  ReadInfo* readInfo, bool returnEmptyBuffer) {
  // Get a new buffer for this read request.
  BaseBuffer* newBuffer;
  if (useByteStreamBuffers) {
    ByteStreamBuffer* buffer = byteStreamBufferFactory.newInstance();
    buffer->clear();

    const StreamInfo& streamInfo = filenameToStreamIDMap->getStreamInfo(
      readInfo->request->path, readInfo->request->offset);

    buffer->setStreamID(streamInfo.getStreamID());

    newBuffer = buffer;
  } else {
    PartitionFile file(readInfo->request->path);
    const std::string& filename = file.getFilename();
    uint64_t fileSize = file.getCurrentSize();

    KVPairBuffer* buffer = kvPairBufferFactory.newInstance(fileSize);
    buffer->setSourceName(filename);
    buffer->setLogicalDiskID(file.getPartitionID());
    buffer->addJobID(file.getJobID());

    newBuffer = buffer;
  }

  if (!returnEmptyBuffer) {
    // Set token if available.
    if (readInfo->token != NULL) {
      newBuffer->setToken(readInfo->token);
      readInfo->token = NULL;
    }

    // Compute the size of the read.
    uint64_t readSize = std::min<uint64_t>(
      newBuffer->getCapacity(),
      readInfo->request->length - bytesRead.at(readInfo->request));

    // Update the ReadInfo and link it to the append pointer.
    readInfo->buffer = newBuffer;
    readInfo->size = readSize;
    const uint8_t* appendPointer = newBuffer->setupAppend(readSize);
    reads[appendPointer] = readInfo;

    // Instruct the AIO implementation to prepare the buffer for reading
    prepareRead(appendPointer, *(readInfo->file), readSize);
    // Begin the first read into the buffer.
    issueNextRead(appendPointer);
  }

  return newBuffer;
}

void AsynchronousReader::serviceIdleBuffers() {
  while (!idleBuffers.empty()) {
    const uint8_t* buffer = idleBuffers.front();
    idleBuffers.pop();

    // If we still have part of this buffer left to read, issue another
    // asynchronous read.
    if (!issueNextRead(buffer)) {
      // This buffer is full
      fullBuffers.push(buffer);
    }
  }
}

void AsynchronousReader::emitFullBuffers() {
  // Emit each full buffer and issue more reads for the associated read request.
  while (!fullBuffers.empty()) {
    const uint8_t* appendPointer = fullBuffers.front();
    fullBuffers.pop();
    ReadInfo* readInfo = reads.at(appendPointer);

    uint64_t& totalBytesRead = bytesRead.at(readInfo->request);
    totalBytesRead += readInfo->size;

    // Sanity check to make sure we read to the right place.
    TRITONSORT_ASSERT(readInfo->file->seek(0, File::FROM_CURRENT) ==
           readInfo->request->offset + totalBytesRead,
           "We've read %llu bytes starting at offset %llu but somehow we ended "
           "up at file position %llu", totalBytesRead,
           readInfo->request->offset,
           readInfo->file->seek(0, File::FROM_CURRENT));

    // Commit the append and emit the buffer.
    readInfo->buffer->commitAppend(appendPointer, readInfo->size);
    logger.add(readSizeStatID, readInfo->buffer->getCurrentSize());
    emitWorkUnit(readInfo->buffer);

    // We no longer need to link this read info to the completed append pointer,
    // but don't delete the read info quite yet because we can still reuse it.
    reads.erase(appendPointer);

    // Start a new buffer if we still have more to read from this request.
    if (totalBytesRead < readInfo->request->length) {
      if (tokenPool != NULL) {
        // We're using tokens to pause reads, so don't immediately issue the
        // next read.
        tokenIDSet.insert(readInfo->tokenID);
        waitingForToken[readInfo->tokenID] = readInfo;
      } else {
        // No pausing necessary, so issue the next read.
        startNextReadBuffer(readInfo);
      }
    } else {
      if (useByteStreamBuffers) {
        // We need to send an empty buffer downstream to manually signal that
        // the stream is closed.
        /// \TODO(MC): We really should be doing something better than using
        /// empty buffers as signals...
        BaseBuffer* emptyBuffer = startNextReadBuffer(readInfo, true);
        emitWorkUnit(emptyBuffer);
      }

      // We've finished the read request, so go ahead and actually free the
      // ReadInfo and File objects
      bytesRead.erase(readInfo->request);
      alignedBytesRead += readInfo->file->getAlignedBytesRead();
      readInfo->file->close();

      if (deleteAfterRead) {
        readInfo->file->unlink();
      }

      delete readInfo->file;
      delete readInfo;
    }
  }
}

void AsynchronousReader::waitForReadCompletionAndEmitBuffers() {
  // Wait for at least one read to complete.
  waitForReadsToComplete(1);
  // Now issue more reads for partially completed buffers.
  serviceIdleBuffers();
  // Finally emit any completely full buffers.
  emitFullBuffers();
}

void AsynchronousReader::checkForReadTokens() {
  bool done = false;
  while (!done && !waitingForToken.empty()) {
    // Try to get a token for any of the waiting files.
    WriteToken* token = tokenPool->attemptGetToken(tokenIDSet);
    if (token != NULL) {
      // We got a token
      uint64_t tokenID = token->getDiskID();
      ReadInfo* readInfo = waitingForToken.at(tokenID);
      readInfo->token = token;

      // Issue the read.
      startNextReadBuffer(readInfo);

      // Remove this read from the waiting set.
      tokenIDSet.erase(tokenID);
      waitingForToken.erase(tokenID);
    } else {
      // Can't get tokens for any of the waiting reads.
      done = true;
    }
  }
}
