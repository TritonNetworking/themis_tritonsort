#!/usr/bin/env python

"""
This script validates the contents of the JSON files that comprise an
application's description.
"""

import os,sys
import json
from optparse import OptionParser
from types import NoneType
import consts

class ValidationFailedError(Exception):
    pass

class DescriptionValidator(object):
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

    def validateStageClassInfo(self, stageClassInfo):
        for key in stageClassInfo.keys():
            currInfo = stageClassInfo[key]

            self.checkHasKVPair((key), currInfo, "impls", list, req=True)

    def validateStructureInfo(self, structureInfo, stageClassInfo):
        self.checkHasKVPair((), structureInfo, consts.PHASES_KEY, list,
                            req=True)

        phases = structureInfo[consts.PHASES_KEY]

        for phase in phases:
            self.checkHasKVPair((), structureInfo, phase, dict, req=True)
            self.validatePhaseInfoObject(phase, structureInfo[phase],
                                         stageClassInfo)

    def validatePhaseInfoObject(self, phaseName, phaseInfoObj, stageClassInfo):
        self.checkHasKVPair((phaseName), phaseInfoObj, consts.STAGES_KEY, dict,
                            req=True)
        self.checkHasKVPair((phaseName), phaseInfoObj,
                            consts.STAGE_TO_STAGE_KEY, list, req=True)

        stageNames = phaseInfoObj[consts.STAGES_KEY].keys()
        for stage in stageNames:
            self.validateStageInfoObj((phaseName, consts.STAGES_KEY, stage),
                                      phaseInfoObj[consts.STAGES_KEY][stage],
                                      stageClassInfo, stageNames)

        for edgeObj in phaseInfoObj[consts.STAGE_TO_STAGE_KEY]:
            self.validateEdgeObj((phaseName, consts.STAGE_TO_STAGE_KEY),
                                 edgeObj, stageNames)

    def validateStageInfoObj(self, parentKeys, stageInfoObj, stageClassInfo,
                             stageNames):
        self.checkHasKVPair(parentKeys, stageInfoObj, consts.TYPE_KEY,
                            [str, unicode], req=True)

        self.require(parentKeys, stageInfoObj[consts.TYPE_KEY] in \
                         stageClassInfo.keys(),\
                         "Can't find information for stage class '%s'" % \
                         (stageInfoObj[consts.TYPE_KEY]))

    def validateEdgeObj(self, parentKeys, edgeObj, stageNames):
        self.checkHasKVPair(parentKeys, edgeObj, consts.SRC_KEY, [str, unicode],
                            req=True)
        self.checkHasKVPair(parentKeys, edgeObj, consts.DEST_KEY,
                            [str, unicode], req=True)

        for key in [consts.SRC_KEY, consts.DEST_KEY]:
            self.require(parentKeys, edgeObj[key] in stageNames, "Unknown "
                         "stage '%s'" % (edgeObj[key]))

    def validateDescription(self, descriptionDir):
        filenames = [consts.STRUCTURE_FILENAME, consts.STAGE_INFO_FILENAME]

        expectedFiles = [os.path.join(descriptionDir, filename)
                         for filename in filenames]

        (structureFile, stagesFile) = expectedFiles

        for filename in expectedFiles:
            if not os.path.exists(filename):
                self.error("Can't find required file '%s'" % (filename))

        structureInfo = self.loadJsonFile(structureFile)
        stageClassInfo = self.loadJsonFile(stagesFile)

        self.filename = stagesFile
        self.validateStageClassInfo(stageClassInfo)

        self.filename = structureFile
        self.validateStructureInfo(structureInfo, stageClassInfo)

def main():
    optionParser = OptionParser(usage="usage: %prog [options] "
                                "<description directory>")

    (options, args) = optionParser.parse_args()

    if len(args) != 1:
        optionParser.error("Incorrect argument count")

    descriptionDir = args[0]

    if not os.path.exists(descriptionDir):
        print >>sys.stderr, "Can't find directory '%s'" % (descriptionDir)

    validator = DescriptionValidator()
    failed = False
    try:
        validator.validateDescription(descriptionDir)
    except ValidationFailedError:
        failed = True

    if not failed:
        validator.printSummary()

if __name__ == "__main__":
    main()
