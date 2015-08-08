#ifndef MAPRED_PHASE_ZERO_SAMPLE_METADATA_H
#define MAPRED_PHASE_ZERO_SAMPLE_METADATA_H

#include <stdint.h>

#include "mapreduce/common/KeyValuePair.h"

class KVPairBuffer;

/**
   Container class that stores metadata about the characteristics of phase zero,
   which is used to compute intermediate data ratios and other information.
   Typically this metadata will be serialized as the first tuple in a phase zero
   buffer.
 */
class PhaseZeroSampleMetadata {
public:
  /// \return the size of a phase zero metadata tuple
  static uint64_t tupleSize();

  /// Constructor
  /**
     \param jobID the ID of the job

     \param tuplesIn the number of tuples that went into the map function

     \param bytesIn the number of bytes that went into the map function

     \param tuplesOut the number of tuples that came out of the map function

     \param bytesOut the number of bytes that actually came out of the map
     function

     \param bytesMapped the number of bytes that the map function tried to
     write, which may differ from bytesOut if reservoir sampling is used
   */
  PhaseZeroSampleMetadata(
    uint64_t jobID, uint64_t tuplesIn, uint64_t bytesIn, uint64_t tuplesOut,
    uint64_t bytesOut, uint64_t bytesMapped);

  /// Constructor
  /**
     \param kvPair a KeyValuePair containing the metadata to load
   */
  PhaseZeroSampleMetadata(const KeyValuePair& kvPair);

  /// Constructor
  /**
     Copy constructor that loads metadata from another metadata object
   */
  PhaseZeroSampleMetadata(const PhaseZeroSampleMetadata& otherMetadata);

  /**
     Write this metadata to a KeyValuePair.

     \param kvPair the KeyValuePair to write to

     \warning this object must persist for the duration that kvPair is used
     since it will contain pointers to private members
   */
  void write(KeyValuePair& kvPair);

  /**
     Merge another metadata object into this one by adding metadata values.

     \param otherMetadata the metadata to merge into this one
   */
  void merge(const PhaseZeroSampleMetadata& otherMetadata);

  /// \return the job ID associated with this metadata
  uint64_t getJobID() const;

  /// \return the number of tuples that went into the map function
  uint64_t getTuplesIn() const;

  /// \return the number of tuples that came out of the map function
  uint64_t getTuplesOut() const;

  /// \return the number of bytes that went into the map function
  uint64_t getBytesIn() const;

  /// \return the number of bytes that came out of the map function
  uint64_t getBytesOut() const;

  /// \return the number of bytes that the map function tried to write, which
  /// may differ from getBytesOut() if reservoir sampling is used
  uint64_t getBytesMapped() const;

  /**
     Compare two metadata objects for equality.

     \param other the other metadata object

     \return true if metadata is the same, false otherwise
   */
  bool equals(const PhaseZeroSampleMetadata& other) const;

private:
  /// Internal structure for representing metadata
  struct Metadata {
    uint64_t jobID;
    uint64_t tuplesIn;
    uint64_t bytesIn;
    uint64_t tuplesOut;
    uint64_t bytesOut;
    uint64_t bytesMapped;
  };

  /// \TODO(MC): Do we still need this to be non-zero?
  static const uint32_t TUPLE_KEY_SIZE = 1;

  /**
     Helper function that computes network order metadata, which is used for
     sending, from the actual metadata.
   */
  void computeNetworkOrderMetadata();

  Metadata metadata;
  Metadata networkOrderMetadata;
};

#endif // MAPRED_PHASE_ZERO_SAMPLE_METADATA_H
