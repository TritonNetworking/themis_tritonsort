# Utility functions for tritonsort related string parsing

from unitconversion import convert, parse

# Parses a data size string such as 1000B, 1K, 500MB etc.
# R is supported for legacy reasons, and only makes sense when
# considering 100 byte records.
def parseDataSize(dataSize):
    parsedDataSize = parse(dataSize)

    if parsedDataSize == None:
        sys.exit("Can't parse data size '%s'" % (dataSize))

    (quantity, unit) = parsedDataSize

    numBytes = convert(quantity, unit, "B")
    return int(numBytes)

def main():
    print parseDataSize("100R")
    print parseDataSize("200MB")
    print parseDataSize("5TB")

if __name__ == "__main__":
    main()
