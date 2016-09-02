#!/usr/bin/env python

import argparse, ConfigParser, json, os, plumbum, string, sys,\
    tempfile, redis, getpass
from plumbum.cmd import gcloud, gsutil
from plumbum import BG

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CLOUD_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))

sys.path.append(CLOUD_DIR)

from auth_utils import authenticate

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir)))

import utils

def run_instances(
        instance_type, num_instances, local_ssds, persistent_ssds, image,
        network, zone, cluster_name, node_type, bucket, project, cluster_ID):
    userdata = {}
    userdata["cluster_ID"] = cluster_ID
    userdata["cluster_name"] = cluster_name
    userdata["node_type"] = node_type
    userdata["bucket"] = bucket

    local_ssds = int(local_ssds)
    persistent_ssds = int(persistent_ssds) if persistent_ssds else 0
    assert local_ssds in xrange(5), "Must specify 0-4 local SSDs per node"
    devices = []
    # devices come up preformatted as /dev/disk/by-id/google-local-ssd-X
    for i in range(local_ssds):
        devices.append("/dev/disk/by-id/google-local-ssd-%d" % (i))
    for i in range(persistent_ssds):
        devices.append("/dev/disk/by-id/google-themis-persistent-disk-%d" % i)
    userdata["devices"] = ",".join(devices)

    cmd = gcloud["compute"]["--project"][project]["instances"]["create"]
    for i in xrange(num_instances):
        cmd = cmd["cluster-%d-%s-%d" % (cluster_ID, node_type, i)]

    cmd = cmd["--zone"][zone]["--machine-type"][instance_type]\
          ["--network"][network]\
          ["--scopes"]["https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control"]\
          ["--metadata"]["^;^userdata=%s" % json.dumps(userdata)]\
          ["--image"]\
          ["https://www.googleapis.com/compute/v1/projects/%s/global/images/%s"\
           % (project, image)]

    for i in xrange(local_ssds):
        cmd = cmd["--local-ssd"]["interface=nvme"]

    cmd()

    # If there's any persistent disks, add them now.
    if persistent_ssds:
        make_disk_cmd = gcloud["compute"]["--project"][project]["disks"]["create"]
        for i in xrange(num_instances):
            for s in xrange(persistent_ssds):
                persistent_disk = "cluster-%d-%s-%d-disk-%d" % (cluster_ID, node_type, i, s)
                make_disk_cmd = make_disk_cmd[persistent_disk]
        make_disk_cmd = make_disk_cmd["--size"]["750"]["--type"]["pd-ssd"]["--zone"][zone]
        make_disk_cmd()

        pending_add_cmds = []
        add_disk_cmd_base = gcloud["compute"]["instances"]["attach-disk"]["--zone"][zone]
        for i in xrange(num_instances):
            for s in xrange(persistent_ssds):
                instance = "cluster-%d-%s-%d" % (cluster_ID, node_type, i)
                persistent_disk = "cluster-%d-%s-%d-disk-%d" % (cluster_ID, node_type, i, s)
                dev_name = "themis-persistent-disk-%d" % s
                add_disk_cmd = add_disk_cmd_base[instance]["--disk"][persistent_disk]["--device-name"][dev_name] & BG
                pending_add_cmds.append(add_disk_cmd)
        for pending_cmd in pending_add_cmds:
            pending_cmd.wait()


def launch_google_cluster(
        cluster_name, cluster_size, instance_type, local_ssds, persistent_ssds, image,
        master_instance_type, network, zone, bucket, private_key, public_key,
        themis_config_directory, provider_info, redis_client):
    private_key = os.path.expanduser(private_key)
    public_key = os.path.expanduser(public_key)
    themis_config_directory = os.path.expanduser(themis_config_directory)

    # In google, zones are regions with a letter appended
    region = zone[:-1]

    if "," in private_key:
        print >>sys.stderr,\
            "Private key path %s is invalid. Cannot contain commas."\
            % private_key
        return 1

    if "," in public_key:
        print >>sys.stderr,\
            "Public key path %s is invalid. Cannot contain commas."\
            % public_key
        return 1

    # Check for existence of the bucket
    print "Configuring storage directory for cluster..."
    try:
        gsutil["ls"]["gs://%s" % bucket]()
    except plumbum.commands.processes.ProcessExecutionError as e:
        print >>sys.stderr, "Storage bucket %s does not exist." % bucket
        return 1

    # Get a unique cluster ID
    print "Getting a unique cluster ID"
    cluster_ID = int(redis_client.incr("cluster_ID"))
    unique = False
    while not unique:
        print "Testing ID %d for uniqueness..." % cluster_ID
        if redis_client.sismember("clusters", str(cluster_ID)):
            # ID exists in redis, get another and try again.
            cluster_ID = int(redis_client.incr("cluster_ID"))
            continue

        # Check for the existence of this cluster in the storage system.
        try:
            cluster_directory = "gs://%s/cluster_%d" % (bucket, cluster_ID)
            print "Checking for %s" % cluster_directory
            gsutil["ls"][cluster_directory]()
            # Cluster exists. Get another and try again.
            cluster_ID = int(redis_client.incr("cluster_ID"))
            continue
        except plumbum.commands.processes.ProcessExecutionError as e:
            # Cluster does not exist.
            unique = True

    print "Unique cluster ID is %d" % cluster_ID

    # Create a "folder" for this cluster
    new_directory = tempfile.NamedTemporaryFile()
    gsutil["cp"][new_directory.name]\
        ["gs://%s/cluster_%d/.dir" % (bucket, cluster_ID)]()
    new_directory.close()

    # Upload cluster.conf and google.conf files to Storage
    try:
        cluster_config_file = tempfile.NamedTemporaryFile()
        parser = ConfigParser.SafeConfigParser()
        parser.add_section("cluster")
        parser.set("cluster", "id", str(cluster_ID))
        parser.set("cluster", "name", cluster_name)
        parser.set("cluster", "cluster_size", "%d" % cluster_size)

        parser.set("cluster", "provider", "google")

        private_key_basename = os.path.basename(private_key)
        public_key_basename = os.path.basename(public_key)

        parser.set("cluster", "private_key", private_key_basename)
        parser.set("cluster", "public_key", public_key_basename)
        parser.set("cluster", "username", provider_info["remote_username"])
        # Log to ~/themis_logs by default
        parser.set("cluster", "log_directory", "~/themis_logs/")
        parser.set("cluster", "config_directory", "~/themis_config/")
        parser.set("cluster", "themis_directory", "~/themis/")
        parser.set("cluster", "disk_mountpoint", "/mnt/disks/")

        parser.write(cluster_config_file)
        cluster_config_file.flush()

        gsutil["cp"][cluster_config_file.name]\
            ["gs://%s/cluster_%d/cluster.conf" % (bucket, cluster_ID)]()
    finally:
        # Make sure we close the temp file.
        cluster_config_file.close()

    try:
        google_config_file = tempfile.NamedTemporaryFile()
        parser = ConfigParser.SafeConfigParser()
        parser.add_section("google")
        parser.set("google", "project", provider_info["project"])
        parser.set("google", "region", region)
        parser.set("google", "network", network)
        parser.set("google", "zone", zone)
        parser.set("google", "image", image)
        parser.set("google", "master_instance_type", master_instance_type)
        parser.set("google", "instance_type", instance_type)
        parser.set("google", "local_ssds", local_ssds)
        parser.set("google", "persistent_ssds", persistent_ssds)
        parser.set("google", "bucket", bucket)

        parser.write(google_config_file)
        google_config_file.flush()

        gsutil["cp"][google_config_file.name]\
            ["gs://%s/cluster_%d/google.conf" % (bucket, cluster_ID)]()
    finally:
        # Make sure we close the temp file.
        google_config_file.close()

    # Cluster creation should succeed, so add information to redis.
    redis_client.sadd("clusters", cluster_ID)
    redis_client.hset("cluster:%d" % cluster_ID, "provider", "google")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "project", provider_info["project"])
    redis_client.hset("cluster:%d" % cluster_ID, "bucket", bucket)
    redis_client.hset("cluster:%d" % cluster_ID, "name", cluster_name)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "region", region)
    redis_client.hset("cluster:%d" % cluster_ID, "network", network)
    redis_client.hset("cluster:%d" % cluster_ID, "zone", zone)
    redis_client.hset("cluster:%d" % cluster_ID, "image", image)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "master_instance_type",
        master_instance_type)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "instance_type", instance_type)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "local_ssds", local_ssds)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "persistent_ssds", persistent_ssds)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "cluster_size", "%d" % cluster_size)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "private_key", private_key_basename)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "public_key", public_key_basename)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "cluster_status", "Booting")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "username", provider_info["remote_username"])
    redis_client.hset(
        "cluster:%d" % cluster_ID, "log_directory", "~/themis_logs/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "config_directory", "~/themis_config/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "themis_directory", "~/themis/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "disk_mountpoint", "/mnt/disks/")

    # Upload themis configuration directory.
    gsutil["-m"]["rsync"]["-r"][themis_config_directory]\
        ["gs://%s/cluster_%d/themis_config" % (bucket, cluster_ID)]()

    # Upload keys
    gsutil["cp"][private_key]\
        ["gs://%s/cluster_%d/keys/%s" % (
            bucket, cluster_ID, private_key_basename)]()
    gsutil["cp"][public_key]\
        ["gs://%s/cluster_%d/keys/%s" % (
            bucket, cluster_ID, public_key_basename)]()

    # Launch master node.
    print "Launching master node..."
    run_instances(
        master_instance_type, 1, 0, 0, image, network, zone, cluster_name,
        "master", bucket, provider_info["project"], cluster_ID)

    # Launch slave nodes.
    print "Launching %d slave nodes..." % cluster_size
    run_instances(
        instance_type, cluster_size, local_ssds, persistent_ssds, image,
        network, zone, cluster_name, "slave", bucket, provider_info["project"],
        cluster_ID)

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Launch a Themis cluster on Google")

    parser.add_argument("config", help="Cloud provider config file")

    parser.add_argument("cluster_name", help="Unique cluster name")
    parser.add_argument(
        "cluster_size", type=int, help="The number of worker nodes")
    parser.add_argument(
        "instance_type", help="VM instance type of the worker nodes")
    parser.add_argument(
        "local_ssds", help="Number of local SSDs to add to each node", type=int)
    parser.add_argument(
        "persistent_ssds", help="Number of persistent SSDs to add to each node", type=int)
    parser.add_argument("image", help="Google Cloud Compute Engine VM Image")
    parser.add_argument(
        "master_instance_type", help="VM instance type of the master node")
    parser.add_argument(
        "network", help="Network to run in")
    parser.add_argument("zone", help="Compute Engine Zone (eg. us-central1-f)")
    parser.add_argument(
        "bucket", help="Storage bucket to use for storing configuration files")
    parser.add_argument(
        "private_key", help="Private key file for ssh")
    parser.add_argument(
        "public_key", help="Public key file for ssh")
    parser.add_argument(
        "themis_config_directory", help="Local directory containing Themis "
        "config files to upload to Storage.")

    utils.add_redis_params(parser)
    args = parser.parse_args()

    provider_info = authenticate("google", args.config)

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    return launch_google_cluster(
        args.cluster_name, args.cluster_size, args.instance_type,
        args.local_ssds, args.persistent_ssds, args.image, args.master_instance_type,
        args.network, args.zone, args.bucket, args.private_key, args.public_key,
        args.themis_config_directory, provider_info, redis_client)

if __name__ == "__main__":
    sys.exit(main())
