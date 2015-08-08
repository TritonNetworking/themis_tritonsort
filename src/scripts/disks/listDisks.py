#!/usr/bin/env python

"""
Simple utility that queries the disks.json file in the same directory for the
machine's disk and mountpoint lists.
usage:
./listDisks.py devices
./listDisks.py mountpoints
"""

from diskdescriptionutils import DiskDescription
from optparse import OptionParser
import os,sys
import socket
import warnings

# Expect the Json file to be in the same directory as this script
DEFAULT_DISK_JSON_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "disks.json")

ILLEGAL_PATH_SUBSTRINGS = ["/mnt/tritonsort", "/mnt/dcswitch", "/home"]

def __listDiskParameter(parameterFunction, hostname, diskJsonFile):
    if hostname == None:
        hostname = __getHostname()

    # DiskDescription constructor validates the json file so we don't have to
    diskDescription = DiskDescription(diskJsonFile)

    outputList = parameterFunction(diskDescription, hostname)

    # Safety check
    for output in outputList:
        for illegalSubstring in ILLEGAL_PATH_SUBSTRINGS:
            if illegalSubstring in output:
                sys.exit("Illegal substring %s found in output %s"
                         % (illegalSubstring, output))

    # Convert from unicode to string object
    return [str(output) for output in outputList]

def listDiskDevices(hostname = None, diskJsonFile = DEFAULT_DISK_JSON_FILE):
    """
    List disk devices for this machine
    """
    return __listDiskParameter(DiskDescription.getDevicesForMachine, hostname,
                               diskJsonFile)

def listDiskMountpoints(hostname = None, diskJsonFile=DEFAULT_DISK_JSON_FILE):
    """
    List disk mountpoints for this machine
    """
    return __listDiskParameter(DiskDescription.getMountpointsForMachine,
                               hostname, diskJsonFile)

def __getHostname():
    # Get hostname
    # host.blah
    hostname = socket.gethostname()
    # host
    hostname = hostname.split(".")[0]

    return hostname

def main():
    validCommands = {"devices" : listDiskDevices,
                     "mountpoints" : listDiskMountpoints}

    usageString = "usage: %prog [options] command\n\nValid Commands:\n"

    for command, fn in validCommands.items():
        usageString += "%s - %s\n" % (command, fn.__doc__.strip())

    optionParser = OptionParser(usage=usageString)
    (options, args) = optionParser.parse_args()

    if len(args) != 1:
        optionParser.error("Incorrect argument count")

    command = args[0]
    if command not in validCommands:
        optionParser.error("Invalid command %s." % (command))

    # Executing this script's main should return the list as one item per line
    outputList = validCommands[command]()

    for output in outputList:
        print output

if __name__ == "__main__":
    main()
