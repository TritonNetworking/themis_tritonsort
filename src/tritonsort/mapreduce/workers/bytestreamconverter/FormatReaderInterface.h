#ifndef MAPRED_FORMAT_READER_INTERFACE_H
#define MAPRED_FORMAT_READER_INTERFACE_H

class ByteStreamBuffer;

/**
   A format reader is a class that encapsulates stream state and processing
   logic for a ByteStreamConverter.
 */
class FormatReaderInterface {
public:
  /// Destructor
  virtual ~FormatReaderInterface() {}

  /**
     Called by a byte stream converter to read a byte stream of a given format
     and convert it to key/value pairs.
   */
  virtual void readByteStream(ByteStreamBuffer& buffer) = 0;
};

#endif // MAPRED_FORMAT_READER_INTERFACE_H
