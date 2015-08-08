#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/FixedSizeKVPairFormatReader.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderFactory.h"
#include "mapreduce/workers/bytestreamconverter/KVPairFormatReader.h"
#include "mapreduce/workers/bytestreamconverter/RdRandFormatReader.h"
#include "mapreduce/workers/bytestreamconverter/TextLineFormatReader.h"

FormatReaderFactory::FormatReaderFactory(
  ByteStreamConverter& _converter, const std::string& _implName,
  const Params& _params, const std::string& _phaseName)
  : implName(_implName),
    params(_params),
    phaseName(_phaseName),
    converter(_converter) {
}

FormatReaderInterface* FormatReaderFactory::newFormatReader(
  const StreamInfo& streamInfo) {

  if (implName == "KVPairFormatReader") {
    return new KVPairFormatReader(streamInfo, converter);
  } else if (implName == "TextLineFormatReader") {
    return new TextLineFormatReader(streamInfo, converter);
  } else if (implName == "FixedSizeKVPairFormatReader") {
    // Use map or reduce specific variables depending on the phase.
    uint32_t keyLength = 0;
    uint32_t valueLength = 0;
    if (phaseName == "phase_zero" || phaseName == "phase_one") {
      keyLength = params.get<uint32_t>("MAP_INPUT_FIXED_KEY_LENGTH");
      valueLength = params.get<uint32_t>("MAP_INPUT_FIXED_VALUE_LENGTH");
    } else if (phaseName == "phase_two" || phaseName == "phase_three") {
      keyLength = params.get<uint32_t>("REDUCE_INPUT_FIXED_KEY_LENGTH");
      valueLength = params.get<uint32_t>("REDUCE_INPUT_FIXED_VALUE_LENGTH");
    } else {
      ABORT("Unknown phase %s", phaseName.c_str());
    }

    return new FixedSizeKVPairFormatReader(
      streamInfo, converter, keyLength, valueLength);
  } else if (implName == "RdRandFormatReader") {
    return new RdRandFormatReader(streamInfo, converter);
  } else {
    ABORT("Unknown format reader type '%s'", implName.c_str());
    return NULL;
  }
}
