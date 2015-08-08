from networkx import DiGraph
from networkx.algorithms.shortest_paths.generic import shortest_path
import re

CONVERSIONS = DiGraph()
CONVERSION_UNITS = set()

def addConversion(source, dest, multiplier):
    CONVERSIONS.add_edge(source, dest,
                         function = lambda x: x * float(multiplier))
    CONVERSIONS.add_edge(dest, source,
                         function = lambda x: x / float(multiplier))
    CONVERSION_UNITS.add(source)
    CONVERSION_UNITS.add(dest)

    for unit in source, dest:
        if unit[-1] == 'b' or unit[-1] == 'B':
            CONVERSION_UNITS.add(unit + "ps")

addConversion('R', 'B', 100)
addConversion('KB', 'B', 1000)
addConversion('MB', 'KB', 1000)
addConversion('GB', 'MB', 1000)
addConversion('TB', 'GB', 1000)
addConversion('KiB', 'B', 1024)
addConversion('MiB', 'B', 1048576)
addConversion('GiB', 'B', 1073741824)
addConversion('TiB', 'B', 1099511627776)
addConversion('s', 'ms', 1000)
addConversion('ms', 'us', 1000)
addConversion('min', 's', 60)
addConversion('B', 'b', 8)
addConversion('Kb', 'b', 1000)
addConversion('Mb', 'Kb', 1000)
addConversion('Gb', 'Mb', 1000)
addConversion('Kib', 'b', 1024)
addConversion('Mib', 'b', 1048576)
addConversion('Gib', 'b', 1073741824)
addConversion('Tib', 'b', 1099511627776)

quantityStringRegex = re.compile(
    "([0-9.]+)\s*(%s)$" % ('|'.join(CONVERSION_UNITS)))

def parse(quantityString):
    quantityString = quantityString.strip()

    match = quantityStringRegex.match(quantityString)

    if match is None:
        return None
    else:
        return (float(match.group(1)), match.group(2))

def convert(quantity, sourceUnit, destUnit):
    # Kludge: if both units are rates per second, convert using the size portion
    # of the rate
    if sourceUnit[-2:] == 'ps' and destUnit[-2:] == 'ps':
        sourceUnit = sourceUnit[:-2]
        destUnit = destUnit[:-2]

    nodes = shortest_path(CONVERSIONS, sourceUnit, destUnit)
    edges = []

    for i in xrange(1, len(nodes)):
        conversionFunction = CONVERSIONS[nodes[i-1]][nodes[i]]["function"]
        quantity = conversionFunction(quantity)

    return quantity

def parse_and_convert(quantityString, destUnit):
    parsed_quantity = parse(quantityString)

    if parsed_quantity == None:
        return None

    quantity, sourceUnit = parsed_quantity

    return convert(quantity, sourceUnit, destUnit)
