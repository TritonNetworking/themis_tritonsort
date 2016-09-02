#include <string.h>

#include "core/ByteOrder.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"

uint64_t PhaseZeroSampleMetadata::tupleSize() {
  return KeyValuePair::tupleSize(TUPLE_KEY_SIZE, sizeof(Metadata));
}

PhaseZeroSampleMetadata::PhaseZeroSampleMetadata(
  uint64_t jobID, uint64_t tuplesIn, uint64_t bytesIn, uint64_t tuplesOut,
  uint64_t bytesOut, uint64_t bytesMapped) {
  metadata.jobID = jobID;
  metadata.tuplesIn = tuplesIn;
  metadata.bytesIn = bytesIn;
  metadata.tuplesOut = tuplesOut;
  metadata.bytesOut = bytesOut;
  metadata.bytesMapped = bytesMapped;

  computeNetworkOrderMetadata();
}

PhaseZeroSampleMetadata::PhaseZeroSampleMetadata(
  const PhaseZeroSampleMetadata& otherMetadata) {
  metadata = otherMetadata.metadata;

  computeNetworkOrderMetadata();
}

PhaseZeroSampleMetadata::PhaseZeroSampleMetadata(const KeyValuePair& kvPair) {
  TRITONSORT_ASSERT(kvPair.getKeyLength() == PhaseZeroSampleMetadata::TUPLE_KEY_SIZE,
         "Expected zero-length key when demarshalling metadata; got length %lu",
         kvPair.getKeyLength());
  TRITONSORT_ASSERT(kvPair.getValueLength() == sizeof(Metadata),
         "Unexpected value size %lu when demarshalling metadata",
         kvPair.getValueLength());

  const Metadata* kvPairMetadata = reinterpret_cast<const Metadata*>(
    kvPair.getValue());

  // Convert numbers from network byte order to host byte order
  metadata.jobID = bigEndianToHost64(kvPairMetadata->jobID);
  metadata.tuplesIn = bigEndianToHost64(kvPairMetadata->tuplesIn);
  metadata.tuplesOut = bigEndianToHost64(kvPairMetadata->tuplesOut);
  metadata.bytesIn = bigEndianToHost64(kvPairMetadata->bytesIn);
  metadata.bytesOut = bigEndianToHost64(kvPairMetadata->bytesOut);
  metadata.bytesMapped = bigEndianToHost64(kvPairMetadata->bytesMapped);

  computeNetworkOrderMetadata();
}

void PhaseZeroSampleMetadata::write(KeyValuePair& kvPair) {
  // Key is 1 byte of garbage, so just use the metadata struct here.
  kvPair.setKey(
    reinterpret_cast<uint8_t*>(&networkOrderMetadata), TUPLE_KEY_SIZE);

  // Value is the entire metadata.
  kvPair.setValue(
    reinterpret_cast<uint8_t*>(&networkOrderMetadata), sizeof(metadata));
}

void PhaseZeroSampleMetadata::merge(
  const PhaseZeroSampleMetadata& otherMetadata) {
  TRITONSORT_ASSERT(otherMetadata.metadata.jobID == metadata.jobID,
         "Can't merge metadata from two different jobs (other %llu, this %llu)",
         otherMetadata.metadata.jobID, metadata.jobID);
  metadata.tuplesIn += otherMetadata.metadata.tuplesIn;
  metadata.bytesIn += otherMetadata.metadata.bytesIn;
  metadata.tuplesOut += otherMetadata.metadata.tuplesOut;
  metadata.bytesOut += otherMetadata.metadata.bytesOut;
  metadata.bytesMapped += otherMetadata.metadata.bytesMapped;
}

uint64_t PhaseZeroSampleMetadata::getJobID() const {
  return metadata.jobID;
}

uint64_t PhaseZeroSampleMetadata::getTuplesIn() const {
  return metadata.tuplesIn;
}

uint64_t PhaseZeroSampleMetadata::getTuplesOut() const {
  return metadata.tuplesOut;
}

uint64_t PhaseZeroSampleMetadata::getBytesIn() const {
  return metadata.bytesIn;
}

uint64_t PhaseZeroSampleMetadata::getBytesOut() const {
  return metadata.bytesOut;
}

uint64_t PhaseZeroSampleMetadata::getBytesMapped() const {
  return metadata.bytesMapped;
}

bool PhaseZeroSampleMetadata::equals(const PhaseZeroSampleMetadata& other)
  const {
  return (metadata.tuplesIn == other.metadata.tuplesIn) &&
    (metadata.tuplesOut == other.metadata.tuplesOut) &&
    (metadata.bytesIn == other.metadata.bytesIn) &&
    (metadata.bytesOut == other.metadata.bytesOut) &&
    (metadata.bytesMapped == other.metadata.bytesMapped);
}

void PhaseZeroSampleMetadata::computeNetworkOrderMetadata() {
  networkOrderMetadata.jobID = hostToBigEndian64(metadata.jobID);
  networkOrderMetadata.tuplesIn = hostToBigEndian64(metadata.tuplesIn);
  networkOrderMetadata.tuplesOut = hostToBigEndian64(metadata.tuplesOut);
  networkOrderMetadata.bytesIn = hostToBigEndian64(metadata.bytesIn);
  networkOrderMetadata.bytesOut = hostToBigEndian64(metadata.bytesOut);
  networkOrderMetadata.bytesMapped = hostToBigEndian64(metadata.bytesMapped);
}
