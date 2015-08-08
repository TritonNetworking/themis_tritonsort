#!/usr/bin/env python

import sys, os, ConfigParser

def read_conf_file(filename, section, fields):
    # If fields is a single value, return a single value.
    # If fields is a list, return a tuple of results

    if type(fields) != list:
        fields = [fields]

    conf_file = os.path.expanduser(os.path.join("~", filename))
    if not os.path.exists(conf_file):
        print >>sys.stderr, "Could not find ~/" % filename
        sys.exit(1)

    parser = ConfigParser.SafeConfigParser()
    parser.read(conf_file)

    results = []
    for field in fields:
        result = parser.get(section, field)
        results.append(result)

    if len(results) == 0:
        return None

    if len(results) == 1:
        return results[0]

    return tuple(results)
