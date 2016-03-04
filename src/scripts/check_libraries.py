#!/usr/bin/env python

import sys

# To check for the presence of more libraries, just add their names and the URL
# where they can be found to this map
required_libraries = {
    "jinja2" : "http://jinja.pocoo.org/",
    "networkx" : "http://networkx.lanl.gov/",
    "bottle" : "http://bottlepy.org/",
    "requests" : "http://www.python-requests.org/"
    }

for library, url in required_libraries.items():
    try:
        lib = __import__(library)
        print "Found %s" % (library),

        if hasattr(lib, "version"):
            print "%s" % (lib.version)
        elif hasattr(lib, "__version__"):
            print "%s" % (lib.__version__)
        else:
            print ""
    except ImportError, e:
        sys.exit("Library '%s' (%s) required but not found." % (library, url))

print "All required libraries found!"
