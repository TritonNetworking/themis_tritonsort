#!/usr/bin/env python

import argparse, sys, redis, os
from instance_utils import get_cluster_status

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

def check_cluster_status(cluster_ID, redis_client):
    provider = redis_client.hget("cluster:%d" % cluster_ID, "provider")
    zone = redis_client.hget("cluster:%d" % cluster_ID, "zone")
    cluster_status = get_cluster_status(provider, cluster_ID, zone)

    num_masters = len(cluster_status["online_masters"])
    num_masters_booting = len(cluster_status["booting_masters"])
    master = cluster_status["master"]
    connectable = cluster_status["master_connectable"]
    num_slaves = len(cluster_status["online_slaves"])
    num_slaves_booting = len(cluster_status["booting_slaves"])
    if num_masters == 1:
        print "Master online"
    else:
        assert num_masters == 0, "Cannot have multiple masters online but "\
            "found %d" % num_masters

        if num_masters_booting == 1:
            print "Master booting"
        else:
            assert num_masters_booting == 0,\
                "Cannot have multiple masters booting but found %d"\
                % num_masters_booting
            print "Master offline"

    if connectable:
        print "Master connectable"
    else:
        print "Master not connectable"

    print "%d slaves online" % num_slaves
    print "%d slaves booting" % num_slaves_booting

    if master != None:
        print ""
        print "Cluster status page: %s:4280" % master[1]
        print "Master internal address: %s" % master[0]

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Check status of a Themis cluster")

    parser.add_argument("cluster_ID", help="ID of the cluster", type=int)

    utils.add_redis_params(parser)
    args = parser.parse_args()

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    return check_cluster_status(args.cluster_ID, redis_client)

if __name__ == "__main__":
    sys.exit(main())
