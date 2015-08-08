#include "mapreduce/functions/map/WEXLinkExtractorMapFunction.h"
#include "mapreduce/common/KeyValuePair.h"

void WEXLinkExtractorMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {

  const uint8_t* wexLine = kvPair.getValue();
  uint32_t wexLineLength = kvPair.getValueLength();

  // WEX lines are tab-delimited. The name of the page is given by the second
  // field on the line, and its contents (in XML form) are given by the fourth
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
  // to the start of the XML

  while (valueIndex < wexLineLength && wexLine[valueIndex++] != '\t') {
  }

  if (valueIndex == wexLineLength) {
    ABORT("Ran off the end of the line while looking for the page XML");
  }

  // We're now at the start of the XML data.
  const char* linkStart = "<link><target>";
  uint32_t linkStartLength = 14;
  char linkEnd = '<';

  uint32_t linkStartIndex = 0;

  while (valueIndex < wexLineLength && wexLine[valueIndex] != '\t') {
    // Search forward, checking for the start of a link
    if (wexLine[valueIndex] == linkStart[linkStartIndex]) {
      linkStartIndex++;
    } else {
      linkStartIndex = 0;
    }

    valueIndex++;

    if (linkStartIndex == linkStartLength) {
      // We've found the start of a link; search forward to the end of the
      // link, and emit a k/v pair whose key is the name of the page and whose
      // value is the linked page

      KeyValuePair kvPair;
      kvPair.setKey(pageName, pageNameLength);
      const uint8_t* link = wexLine + valueIndex;
      uint32_t startIndex = valueIndex;

      while (valueIndex < wexLineLength && wexLine[valueIndex++] != linkEnd) {
      }

      if (valueIndex == wexLineLength) {
        return;
      }

      kvPair.setValue(link, valueIndex - startIndex - 1);
      writer.write(kvPair);
    }
  }
}
