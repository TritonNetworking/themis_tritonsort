import os,sys,json
import validateDiskDescription
import consts

class DiskDescription(object):    
    def __init__(self, diskFile):
        if not os.path.exists(diskFile):
            print >>sys.stderr, "Can't find disk json file '%s'" % \
                (diskFile)
            sys.exit(1)
            
        validator = validateDiskDescription.DiskDescriptionValidator()
        
        validator.validateDescription(diskFile)
        
        # If this didn't throw, description is valid
        self.diskInfo = self._getFileJsonContents(diskFile)
        
    def _getFileHandle(self, filename):
        # Don't need to prepend with a description directory
        # Just load the file directly
        totalPath = os.path.abspath(filename)
        if not os.path.exists(totalPath):
            print >>sys.stderr, "Can't find file '%s'" % (totalPath)
            
        fp = open(totalPath, 'r')
        return fp
    
    
    @staticmethod
    def _loadJsonFile(fp):
        try:
            jsonObj = json.load(fp)
        except ValueError as e:
            print >>sys.stderr, "Parsing JSON file '%s' failed: %s" % \
                (filename, e)
            sys.exit(1)
        finally:
            fp.close()

        return jsonObj
            
    def _getFileJsonContents(self, filename):
        fp = self._getFileHandle(filename)
        jsonObj = DiskDescription._loadJsonFile(fp)
        return jsonObj

    # Private Json accessors
    def _getListForProfile(self, profileName, listKey):
        profiles = self.diskInfo[consts.PROFILES_KEY]

        if profileName in profiles:
            profile = profiles[profileName]
            return profile[listKey]
        else:
            print >>sys.stderr, "Cannot get %s for profile %s."\
                "Profile not found." % (listKey, profileName)

    def _getDevicesForProfile(self, profileName):
        return self._getListForProfile(profileName, consts.DEVICES_KEY)

    def _getMountpointsForProfile(self, profileName):
        return self._getListForProfile(profileName, consts.MOUNTPOINTS_KEY)
    
    def _getProfileForMachine(self, machineName):
        machines = self.diskInfo[consts.MACHINES_KEY]
        if machineName in machines:
            # Load the custom profile for this machine
            profileName = machines[machineName]
        else:
            # Load the default profile for this machine
            profileName = consts.DEFAULT_KEY
        return profileName

    # Public Json accessors
    def getDevicesForMachine(self, machineName):
        profileName = self._getProfileForMachine(machineName)
        return self._getDevicesForProfile(profileName)

    def getMountpointsForMachine(self, machineName):
        profileName = self._getProfileForMachine(machineName)
        return self._getMountpointsForProfile(profileName)
