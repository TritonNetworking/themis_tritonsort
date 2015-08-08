#ifndef THEMIS_FORMAT_READER_FACTORY_H
#define THEMIS_FORMAT_READER_FACTORY_H

#include <stdint.h>
#include <string>

class ByteStreamConverter;
class FormatReaderInterface;
class StreamInfo;

/**
   This class is responsible for constructing the appropriate format reader
   based on configuration parameters
 */
class FormatReaderFactory {
public:
  /// Constructor
  /**
     \param converter the byte stream converter that will provide callbacks for
     new format readers

     \param implName the name of the parameter whose value is the format reader
     implementation name corresponding to the format reader that the factory
     should construct

     \params params the global params object

     \param phaseName the name of the phase
   */
  FormatReaderFactory(
    ByteStreamConverter& converter, const std::string& implName,
    const Params& params, const std::string& phaseName);

  /// Create a new format reader whose input is a stream of file data
  /**
     \param streamInfo a StreamInfo object describing the stream

     \return a new format reader meeting specifications
   */
  FormatReaderInterface* newFormatReader(const StreamInfo& streamInfo);

private:
  const std::string implName;
  const Params& params;
  const std::string phaseName;

  ByteStreamConverter& converter;
};

#endif // THEMIS_FORMAT_READER_FACTORY_H
