#include "core/ByteOrder.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/reduce/ClickLogSessionSummarizerReduceFunction.h"

ClickLogSessionSummarizerReduceFunction::
ClickLogSessionSummarizerReduceFunction(uint64_t _sessionTimeThreshold)
  : sessionTimeThreshold(_sessionTimeThreshold) {
}

void ClickLogSessionSummarizerReduceFunction::reduce(
  const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
  KVPairWriterInterface& writer) {

  KeyValuePair kvPair;

  uint64_t startTimestamp = 0;
  const uint8_t* startURL = NULL;
  uint32_t startURLSize = 0;

  while (iterator.next(kvPair)) {
    const uint8_t* value = kvPair.getValue();
    uint32_t valueSize = kvPair.getValueLength();

    uint64_t timestamp = bigEndianToHost64(
      *(reinterpret_cast<const uint64_t*>(value)));
    const uint8_t* url = value + sizeof(timestamp);
    uint32_t urlSize = valueSize - sizeof(timestamp);

    if (startURL == NULL) {
      startTimestamp = timestamp;
      startURL = url;
      startURLSize = urlSize;
    }

    if (timestamp - startTimestamp >= sessionTimeThreshold) {
      // Found the end of the current session; write out <start time, stop
      // time, start URL size, start URL, stop URL size, stop URL>

      uint32_t valueSize = sizeof(SessionHeader) + startURLSize + urlSize;

      uint8_t* writeValPtr = writer.setupWrite(key, keyLength, valueSize);

      uint8_t* currentWriteValPtr = writeValPtr;

      SessionHeader* sessionHeader = reinterpret_cast<SessionHeader*>(
        writeValPtr);

      sessionHeader->firstTimestamp = hostToBigEndian64(startTimestamp);
      sessionHeader->lastTimestamp = hostToBigEndian64(timestamp);
      sessionHeader->firstURLSize = hostToBigEndian64(startURLSize);
      sessionHeader->lastURLSize = hostToBigEndian64(urlSize);

      currentWriteValPtr += sizeof(SessionHeader);
      memcpy(currentWriteValPtr, startURL, startURLSize);
      currentWriteValPtr += startURLSize;
      memcpy(currentWriteValPtr, url, urlSize);

      writer.commitWrite(valueSize);

      // Want to pick a new start URL the next time we see a tuple for this user
      startURL = NULL;
      startURLSize = 0;
    }
  }
}
