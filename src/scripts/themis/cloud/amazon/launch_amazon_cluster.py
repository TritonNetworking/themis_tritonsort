#!/usr/bin/env python

import argparse, ConfigParser, json, os, plumbum, string, sys,\
    tempfile, redis
from plumbum.cmd import aws

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
CLOUD_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))

sys.path.append(CLOUD_DIR)

from auth_utils import authenticate

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

INSTANCE_TYPE_CONFIG = os.path.join(SCRIPT_DIR, "instance_type_info")
if not os.path.exists(INSTANCE_TYPE_CONFIG):
    print >>sys.stderr, "Could not find instance_type_info"
    sys.exit(1)

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir)))

import utils

def run_instances(
        instance_type, num_instances, AMI_ID, subnet_ID, security_group_ID,
        placement_group, EBS_optimized, cluster_name,
        node_type, aws_access_key_id, aws_secret_access_key, s3_bucket,
        cluster_ID):
    # Load instance type and device information
    print "Gathering information for instance devices..."
    parser = ConfigParser.SafeConfigParser()
    parser.read(INSTANCE_TYPE_CONFIG)

    device_map = {}
    instances = []
    for instance, num_devices in parser.items("devices"):
        device_map[instance] = num_devices
        instances.append(instance)

    placement_groups_map = {}
    for instance_type_key, placement_groups_enabled \
        in parser.items("placement_groups"):
        placement_groups_map[instance_type_key] = placement_groups_enabled

    ebs_optimized_map = {}
    for instance_type_key, ebs_optimized_enabled \
        in parser.items("EBS_optimized"):
        ebs_optimized_map[instance_type_key] = ebs_optimized_enabled

    assert instance_type in instances,\
        "Invalid instance type %s" % instance_type
    num_devices = int(device_map[instance_type])

    # Compute the block device mapping from the number of devices. We only
    # support at most 25 devices.
    device_mapping = []
    for device_number, device_letter in \
        zip(xrange(num_devices), string.ascii_lowercase[1:26]):
        device_mapping.append(
            {"DeviceName" : "/dev/xvd%s" % device_letter,
             "VirtualName" : "ephemeral%d" % device_number})

    devices = [x["DeviceName"] for x in device_mapping]

    userdata = {}
    userdata["cluster_ID"] = cluster_ID
    userdata["cluster_name"] = cluster_name
    userdata["node_type"] = node_type
    userdata["aws_access_key_id"] = aws_access_key_id
    userdata["aws_secret_access_key"] = aws_secret_access_key
    userdata["s3_bucket"] = s3_bucket
    userdata["devices"] = ",".join(devices)

    cmd = aws["ec2"]["--profile"]["themis"]["run-instances"]\
          ["--security-group-ids"][security_group_ID]\
          ["--subnet-id"][subnet_ID]\
          ["--user-data"][json.dumps(userdata)]\
          ["--count"][num_instances]\
          ["--instance-type"][instance_type]\
          ["--image-id"][AMI_ID]\
          ["--block-device-mappings"][json.dumps(device_mapping)]

    if EBS_optimized and ebs_optimized_map[instance_type] == "YES":
        cmd = cmd["--ebs-optimized"]
    if placement_group != None and placement_groups_map[instance_type] == "YES":
        cmd = cmd["--placement"]["GroupName=%s" % placement_group]

    cmd()

def launch_amazon_cluster(
        provider_info, cluster_name, cluster_size, instance_type, AMI_ID,
        master_instance_type, subnet_ID, security_group_ID, S3_bucket,
        private_key, public_key, themis_config_directory, placement_group,
        EBS_optimized, username, redis_client):
    aws_access_key_id = provider_info["aws_access_key_id"]
    aws_secret_access_key = provider_info["aws_secret_access_key"]
    region = provider_info["region"]
    private_key = os.path.expanduser(private_key)
    public_key = os.path.expanduser(public_key)
    themis_config_directory = os.path.expanduser(themis_config_directory)

    # Determine zone from subnet
    subnets = aws["--profile"]["themis"]["ec2"]["describe-subnets"]()
    subnets = json.loads(subnets)
    subnets = subnets["Subnets"]
    subnet = [s for s in subnets if s["SubnetId"] == subnet_ID][0]
    zone = subnet["AvailabilityZone"]

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

    # Check for existence of the S3 bucket
    print "Configuring S3 directory for cluster..."
    try:
        aws["--profile"]["themis"]["s3"]["ls"]["s3://%s" % S3_bucket]()
    except plumbum.commands.processes.ProcessExecutionError as e:
        print >>sys.stderr, "S3 bucket %s does not exist." % S3_bucket
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

        # Check for the existence of this cluster in S3.
        cluster_directory = "cluster_%d/" % cluster_ID
        return_code, stdout, stderr = \
          aws["--profile"]["themis"]["s3api"]["list-objects"]["--bucket"]\
          [S3_bucket]["--prefix"][cluster_directory].run()

        if return_code != 0:
            print >>sys.stderr, stderr
            return return_code

        result = json.loads(stdout)
        matched = False
        for listed_file in result["Contents"]:
            key = str(listed_file["Key"])
            if key == cluster_directory:
                matched = True

        if matched:
            # ID exists on S3, get another and try again
            cluster_ID = int(redis_client.incr("cluster_ID"))
            continue

        unique = True

    print "Unique cluster ID is %d" % cluster_ID


    # Create a "folder" for this cluster
    aws["--profile"]["themis"]["s3api"]["put-object"]["--bucket"][S3_bucket]\
        ["--key"][cluster_directory]()

    # Upload cluster.conf and amazon.conf files to S3
    try:
        cluster_config_file = tempfile.NamedTemporaryFile()
        parser = ConfigParser.SafeConfigParser()
        parser.add_section("cluster")
        parser.set("cluster", "id", str(cluster_ID))
        parser.set("cluster", "name", cluster_name)

        parser.set("cluster", "provider", "amazon")

        parser.set("cluster", "cluster_size", "%d" % cluster_size)

        private_key_basename = os.path.basename(private_key)
        public_key_basename = os.path.basename(public_key)

        parser.set("cluster", "private_key", private_key_basename)
        parser.set("cluster", "public_key", public_key_basename)
        parser.set("cluster", "username", username)
        # Log to ~/themis_logs by default
        parser.set("cluster", "log_directory", "~/themis_logs/")
        parser.set("cluster", "config_directory", "~/themis_config/")
        parser.set("cluster", "themis_directory", "~/themis/")
        parser.set("cluster", "disk_mountpoint", "/mnt/disks/")

        parser.write(cluster_config_file)
        cluster_config_file.flush()

        aws["--profile"]["themis"]["s3api"]["put-object"]["--bucket"]\
            [S3_bucket]["--key"]["%scluster.conf" % cluster_directory]\
            ["--body"][cluster_config_file.name]()
    finally:
        # Make sure we close the temp file.
        cluster_config_file.close()

    try:
        amazon_config_file = tempfile.NamedTemporaryFile()
        parser = ConfigParser.SafeConfigParser()
        parser.add_section("amazon")
        parser.set("amazon", "region", region)
        parser.set("amazon", "subnet", subnet_ID)
        parser.set("amazon", "zone", zone)
        parser.set("amazon", "security_group", security_group_ID)
        parser.set("amazon", "ami", AMI_ID)
        parser.set("amazon", "master_instance_type", master_instance_type)
        parser.set("amazon", "instance_type", instance_type)
        parser.set("amazon", "bucket", S3_bucket)

        parser.write(amazon_config_file)
        amazon_config_file.flush()

        aws["--profile"]["themis"]["s3api"]["put-object"]["--bucket"]\
            [S3_bucket]["--key"]["%samazon.conf" % cluster_directory]\
            ["--body"][amazon_config_file.name]()
    finally:
        # Make sure we close the temp file.
        amazon_config_file.close()

    # Cluster creation should succeed, so add information to redis.
    redis_client.sadd("clusters", cluster_ID)
    redis_client.hset("cluster:%d" % cluster_ID, "provider", "amazon")
    redis_client.hset("cluster:%d" % cluster_ID, "bucket", S3_bucket)
    redis_client.hset("cluster:%d" % cluster_ID, "name", cluster_name)
    redis_client.hset("cluster:%d" % cluster_ID, "region", region)
    redis_client.hset("cluster:%d" % cluster_ID, "subnet", subnet_ID)
    redis_client.hset("cluster:%d" % cluster_ID, "zone", zone)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "security_group", security_group_ID)
    redis_client.hset("cluster:%d" % cluster_ID, "AMI", AMI_ID)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "master_instance_type",
        master_instance_type)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "instance_type", instance_type)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "cluster_size", "%d" % cluster_size)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "private_key", private_key_basename)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "public_key", public_key_basename)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "cluster_status", "Booting")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "username", username)
    redis_client.hset(
        "cluster:%d" % cluster_ID, "log_directory", "~/themis_logs/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "config_directory", "~/themis_config/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "themis_directory", "~/themis/")
    redis_client.hset(
        "cluster:%d" % cluster_ID, "disk_mountpoint", "/mnt/disks/")

    # Upload themis configuration directory.
    aws["--profile"]["themis"]["s3"]["sync"]["--exact-timestamps"]\
        [themis_config_directory]\
        ["s3://%s/cluster_%d/themis_config/" % (S3_bucket, cluster_ID)]()

    # Upload keys
    aws["--profile"]["themis"]["s3api"]["put-object"]["--bucket"]\
        [S3_bucket]["--key"]["%skeys/%s" % (
            cluster_directory, private_key_basename)]\
        ["--body"][private_key]()
    aws["--profile"]["themis"]["s3api"]["put-object"]["--bucket"]\
        [S3_bucket]["--key"]["%skeys/%s" % (
            cluster_directory, public_key_basename)]\
        ["--body"][public_key]()

    # Launch master node.
    print "Launching master node..."
    run_instances(
        master_instance_type, 1, AMI_ID, subnet_ID, security_group_ID,
        placement_group, EBS_optimized, cluster_name, "master",
        aws_access_key_id, aws_secret_access_key, S3_bucket, cluster_ID)

    # Launch slave nodes.
    print "Launching %d slave nodes..." % cluster_size
    run_instances(
        instance_type, cluster_size, AMI_ID, subnet_ID, security_group_ID,
        placement_group, EBS_optimized, cluster_name, "slave",
        aws_access_key_id, aws_secret_access_key, S3_bucket, cluster_ID)

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Launch a Themis cluster on EC2")

    parser.add_argument("config", help="Cloud provider config file")

    parser.add_argument("cluster_name", help="Unique cluster name")
    parser.add_argument(
        "cluster_size", type=int, help="The number of worker nodes")
    parser.add_argument(
        "instance_type", help="VM instance type of the worker nodes")
    parser.add_argument("AMI_ID", help="Amazon Machine Image")
    parser.add_argument(
        "master_instance_type", help="VM instance type of the master node")
    parser.add_argument(
        "subnet_ID", help="Subnet IDS for launch")
    parser.add_argument("security_group_ID", help="Security Group ID")
    parser.add_argument(
        "S3_bucket", help="S3 bucket to use for storing configuration files")
    parser.add_argument(
        "private_key", help="Private key file for ssh")
    parser.add_argument(
        "public_key", help="Public key file for ssh")
    parser.add_argument(
        "themis_config_directory", help="Local directory containing Themis "
        "config files to upload to S3.")
    parser.add_argument(
        "--placement_group", help="The optional placement group to use")
    parser.add_argument(
        "--EBS_optimized", action="store_true", default=False,
        help="Launch VMs with EBS optimization on")
    parser.add_argument(
        "--username", default="ec2-user",
        help="Username to use for logging into EC2. Default %(default)s")

    utils.add_redis_params(parser)
    args = parser.parse_args()

    provider_info = authenticate("amazon", args.config)

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    return launch_amazon_cluster(
        provider_info, args.cluster_name, args.cluster_size, args.instance_type,
        args.AMI_ID, args.master_instance_type, args.subnet_ID,
        args.security_group_ID, args.S3_bucket, args.private_key,
        args.public_key, args.themis_config_directory, args.placement_group,
        args.EBS_optimized, args.username, redis_client)

if __name__ == "__main__":
    sys.exit(main())
