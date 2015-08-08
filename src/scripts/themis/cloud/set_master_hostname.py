#!/usr/bin/env python

###
# This script is intended to run on each Themis node AFTER the master is
# confirmed to be online. It sets the master's hostname in the node's
# cluster.conf file so slaves can find the master.
###

import sys, os, ConfigParser

from instance_utils import get_cluster_status

CLUSTER_CONFIG = os.path.expanduser("~/cluster.conf")

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, "cluster")))

from conf_utils import read_conf_file

def main():
    # Read the cluster config to get cluster ID.
    parser = ConfigParser.SafeConfigParser()
    parser.read(CLUSTER_CONFIG)

    cluster_ID = int(parser.get("cluster", "id"))
    provider = parser.get("cluster", "provider")

    zone = read_conf_file("%s.conf" % provider, provider, "zone")

    # Store master address information
    master = get_cluster_status(provider, cluster_ID, zone)["master"]
    if master == None:
        print >>sys.stderr, "Could not find master hostname"
        return 1

    # Set master hostname in cluster.conf
    parser.set("cluster", "master_internal_address", master[0])
    parser.set("cluster", "master_external_address", master[1])

    with open(CLUSTER_CONFIG, "w") as config_file:
        parser.write(config_file)

    return 0

if __name__ == "__main__":
    sys.exit(main())
