#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/functions/map/WEXTextExtractorMapFunction.h"

void WEXTextExtractorMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {

  const uint8_t* wexLine = kvPair.getValue();
  uint32_t wexLineLength = kvPair.getValueLength();

  // WEX lines are tab-delimited. The name of the page is given by the second
  // field on the line, and its contents (in text form) are given by the fifth
  // field.

  uint64_t valueIndex = 0;

  // Move forward in the value until you encounter a tab
  while (valueIndex < wexLineLength && wexLine[valueIndex++] != '\t') {
  }

  // If we ran off the end of the value looking for a tab (because the line was
  // malformed or something), scream really loud
  if (valueIndex == wexLineLength) {
    ABORT("Ran off the end of the line while looking for the page name");
  }

  const uint8_t* pageName = wexLine + valueIndex;
  uint32_t pageNameLength = 0;

  uint32_t startIndex = valueIndex;

  // Now scan forward until we find the end of the name
  while (valueIndex < wexLineLength && wexLine[valueIndex++] != '\t') {
  }

  if (valueIndex == wexLineLength) {
    ABORT("Ran off the end of the line while looking for the datestamp");
  }

  pageNameLength = valueIndex - startIndex - 1;

  // Now that we've captured the page's name, seek forward over the datestamp
  // and XML to the start of the article text.

  while (valueIndex < wexLineLength && wexLine[valueIndex++] != '\t') {
  }

  if (valueIndex == wexLineLength) {
    ABORT("Ran off the end of the line while looking for the page XML");
  }

  while (valueIndex < wexLineLength && wexLine[valueIndex++] != '\t') {
  }

  // We're now at the start of the article text, so we emit a tuple if we have a
  // 5th column
  if (valueIndex < wexLineLength) {
    KeyValuePair outputTuple;
    outputTuple.setKey(pageName, pageNameLength);
    outputTuple.setValue(wexLine + valueIndex, wexLineLength - valueIndex);
    writer.write(outputTuple);
  }
}
