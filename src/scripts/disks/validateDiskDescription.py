#!/usr/bin/env python

"""
This script validates the contents of the JSON file that describes the disk
device and mountpoint configuration of machines in the cluster.
"""

import os,sys
import json
from optparse import OptionParser
from types import NoneType
import consts

class ValidationFailedError(Exception):
    pass

class DiskDescriptionValidator(object):
    def __init__(self):
        # As the validator runs, it counts the number of warnings and errors it
        # encounters.
        self.warnings = 0
        self.errors = 0
        
        # Set self.filename to the file that you're validating before you start
        # validating it so that warning messages can give the filename for
        # context
        self.filename = None

    def printSummary(self):
        print "Warnings: %d" % (self.warnings)
        print "Errors: %d" % (self.errors)

    def parentKeysAsString(self, parentKeys):
        return "/" + "/".join(parentKeys)
        
    def warn(self, warnString):
        print >>sys.stderr, "WARNING: %s" % warnString
        self.warnings += 1
    
    def error(self, errString):
        print >>sys.stderr, "ERROR: %s" % errString
        self.errors += 1
        self.printSummary()
        raise ValidationFailedError
    
    def require(self, parentKeys, condition, errString):
        if not condition:
            self.error(self.parentKeysAsString(parentKeys) + ": " + errString)
            
    def checkHasKVPair(self, parentKeys, infoObj, key, typenames, req=False):
        """
        Check if a key is defined in the given object. If 'req' is True, throw
        an error if the key isn't found, otherwise just print a warning. 

        If the key exists but the type of the key doesn't match what we expect, 
        throw an error unconditionally
        """

        if type(typenames) is not list and type(typenames) is not tuple:
            typenames = [typenames]
        
        parentKeyStr = "/" + "/".join(parentKeys)
        
        if key not in infoObj:
            errStr = "%s: %s does not have a value for key '%s'" % \
                (self.filename, parentKeyStr, key)
            if req:
                self.error(errStr)
            else:
                self.warn(errStr)
        elif type(infoObj[key]) not in typenames:
            self.error("%s: key %s of %s has a different type than we expect. "
                       "Was expecting %s, but encountered type %s" % \
                           (self.filename, key, parentKeyStr, 
                            ",".join([str(t) for t in typenames]), 
                            type(infoObj[key])))
            
            
    def loadJsonFile(self, filename):
        fp = open(filename)
        
        try:
            jsonObj = json.load(fp)
        except ValueError as e:
            self.error("Parsing JSON file '%s' failed: %s" % (filename, e))
        finally:
            fp.close()

        return jsonObj

    def validateDiskInfo(self, diskInfo):
        # Check profiles
        self.checkHasKVPair((), diskInfo, consts.PROFILES_KEY, dict, req=True)
        profiles = diskInfo[consts.PROFILES_KEY]
        # Make sure there is a default profile
        self.checkHasKVPair((consts.PROFILES_KEY,), profiles, 
                            consts.DEFAULT_KEY, dict, req=True)

        for profileName, profile in profiles.iteritems():
            self.checkHasKVPair((consts.PROFILES_KEY, profileName), profile, 
                                consts.DEVICES_KEY, list, req=True)
            devices = profile[consts.DEVICES_KEY]
            self.checkHasKVPair((consts.PROFILES_KEY, profileName), profile, 
                                consts.MOUNTPOINTS_KEY, list, req=True)
            mountpoints = profile[consts.MOUNTPOINTS_KEY]
            # Make sure you have a 1-1 correspondence between devices and 
            # mountpoints 
            self.require((consts.PROFILES_KEY, profileName), 
                         len(devices) == len(mountpoints), 
                         "Must have equal number of devices and mountpoints")

        # Check machines
        self.checkHasKVPair((), diskInfo, consts.MACHINES_KEY, dict, req=True)
        machines = diskInfo[consts.MACHINES_KEY]
        for machineName, profile in machines.iteritems():
            # Verify this profile exists
            self.require((consts.MACHINES_KEY, machineName),
                         profile in profiles,
                         "Must point to a valid profile name")
            
    def validateDescription(self, diskJsonFile):
        diskInfo = self.loadJsonFile(diskJsonFile)
        
        self.filename = diskJsonFile
        self.validateDiskInfo(diskInfo)
                 
def main():
    optionParser = OptionParser(usage="usage: %prog [options] "
                                "<disk json file>")

    (options, args) = optionParser.parse_args()
    
    if len(args) != 1:
        optionParser.error("Incorrect argument count")
    
    diskJsonFile = args[0]
    
    if not os.path.exists(diskJsonFile):
        print >>sys.stderr, "Can't find disk json file '%s'" % (diskJsonFile)
        
    validator = DiskDescriptionValidator()
    failed = False
    try:
        validator.validateDescription(diskJsonFile)
    except ValidationFailedError:
        failed = True
        
    if not failed:
        validator.printSummary()

if __name__ == "__main__":
    main()
