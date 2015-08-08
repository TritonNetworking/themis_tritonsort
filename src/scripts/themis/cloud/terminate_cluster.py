#!/usr/bin/env python

import argparse, plumbum, sys, redis, os
from instance_utils import get_instance_names

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

def terminate_cluster(cluster_ID, redis_client, provider=None, zone=None):
    # Make sure we know about this cluster in redis or provider and zone exist
    if not redis_client.sismember("clusters", str(cluster_ID)):
        if provider not in ["google", "amazon"]:
            print >>sys.stderr, "Cluster %d doesn't exist in redis. "\
                "Set --provider flag to force termination." % cluster_ID
            return 1
        if provider == "google" and zone == None:
            print >>sys.stderr, "Google requires the --zone flag."
            return 1
    else:
        provider = redis_client.hget("cluster:%d" % cluster_ID, "provider")
        if provider == "google":
            zone = redis_client.hget("cluster:%d" % cluster_ID, "zone")

    # Get list of running instances for this cluster.
    print "Gathering list of running instances for %d..." % cluster_ID
    instances = get_instance_names(provider, cluster_ID, zone)

    print "Retrieved list of running instances: %s" % instances

    if len(instances) > 0:
        print "Terminating running instances..."
        if provider == "google":
            gcloud = plumbum.local["gcloud"]
            cmd = gcloud["compute"]["instances"]["delete"]["--zone"][zone]\
                  ["--delete-disks"]["boot"]
            for instance_name in instances:
                cmd = cmd[instance_name]
            cmd()
        elif provider == "amazon":
            aws = plumbum.local["aws"]
            cmd = aws["--profile"]["themis"]["ec2"]["terminate-instances"]\
                  ["--instance-ids"]
            for instance_ID in instances:
                cmd = cmd[instance_ID]
            cmd()
        else:
            print >>sys.stderr, "Provider must be google or amazon"
            return 1

    # Remove cluster information from redis.
    redis_client.delete("cluster:%d" % cluster_ID)
    redis_client.srem("clusters", str(cluster_ID))

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Terminate a Themis cluster")

    parser.add_argument("cluster_ID", help="Unique cluster ID", type=int)
    parser.add_argument(
        "--provider", help="The provider to use (amazon or google). Must be "
        "set if cluster is not found in redis.")
    parser.add_argument(
        "--zone", help="zone that the cluster is running in. Must be set if "
        "cluster is not found in redis and provider is google.")

    utils.add_redis_params(parser)
    args = parser.parse_args()

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    return terminate_cluster(
        args.cluster_ID, redis_client, args.provider, args.zone)

if __name__ == "__main__":
    sys.exit(main())
