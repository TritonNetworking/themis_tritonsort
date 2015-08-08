#!/usr/bin/env python

import sys, argparse, bottle, jinja2, os, time, socket, redis, json,\
    ConfigParser, paramiko, plumbum
from plumbum.cmd import fping
from plumbum import BG
from plumbum.commands.processes import ProcessExecutionError

import plumbum

from auth_utils import authenticate
from instance_utils import get_cluster_status, get_instance_addresses
from terminate_cluster import terminate_cluster

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir)))

import constants

AMAZON_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "amazon"))
sys.path.append(AMAZON_DIR)

from launch_amazon_cluster import launch_amazon_cluster

INSTANCE_TYPE_CONFIG = os.path.join(AMAZON_DIR, "instance_type_info")
if not os.path.exists(INSTANCE_TYPE_CONFIG):
    print >>sys.stderr, "Could not find instance_type_info"
    sys.exit(1)

GOOGLE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "google"))
sys.path.append(GOOGLE_DIR)

from launch_google_cluster import launch_google_cluster


DEFAULT_CONFIG_DIR = os.path.join(SCRIPT_DIR, "default_configs")

provider_info = {}
redis_client = None
cluster_info = None

def wait_on_background_command(background_command):
    background_command.wait()
    if background_command.returncode != 0:
        print >>sys.stderr, background_command.stderr
        sys.exit(background_command.returncode)

    return background_command.stdout

def datetimeformat(value, format='%m-%d-%Y %H:%M:%S'):
    return time.strftime(format, time.localtime(float(value)))

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.abspath(__file__))), trim_blocks=True)

jinja_env.filters["datetime"] = datetimeformat

def display_error(error):
    template = jinja_env.get_template("display_error.jinja2")
    return template.render(error=error)

@bottle.route('/static/<path:path>')
def static(path):
    return bottle.static_file(path, root=os.path.join(SCRIPT_DIR, "static"))

@bottle.route('/', method='POST')
def modify_clusters():
    global redis_client, aws_access_key_id, aws_secret_access_key,\
        cluster_info, provider_info
    print "Modifying Clusters"
    if bottle.request.POST.new_amazon_cluster:
        print "Launching new Amazon cluster"
        cluster_name = bottle.request.POST.cluster_name
        cluster_size = int(bottle.request.POST.cluster_size)
        instance_type = bottle.request.POST.instance_type
        AMI_ID = bottle.request.POST.AMI_ID
        master_instance_type = bottle.request.POST.master_instance_type
        subnet_ID = bottle.request.POST.subnet_ID
        security_group_ID = bottle.request.POST.security_group_ID
        S3_bucket = bottle.request.POST.S3_bucket
        private_key = bottle.request.POST.private_key
        public_key = bottle.request.POST.public_key
        username = bottle.request.POST.username
        themis_config_directory = bottle.request.POST.themis_config_directory
        placement_group = bottle.request.POST.placement_group
        if placement_group == "":
            placement_group = None
        EBS_optimized = bottle.request.POST.EBS_optimized
        if EBS_optimized == "Yes":
            EBS_optimized = True
        else:
            EBS_optimized = False

        try:
            launch_amazon_cluster(
                provider_info["amazon"], cluster_name, cluster_size,
                instance_type, AMI_ID, master_instance_type, subnet_ID,
                security_group_ID, S3_bucket, private_key, public_key,
                themis_config_directory, placement_group, EBS_optimized,
                username, redis_client)
        except ProcessExecutionError as e:
            return display_error(e)

    elif bottle.request.POST.new_google_cluster:
        print "Launching new Google cluster"
        cluster_name = bottle.request.POST.cluster_name
        cluster_size = int(bottle.request.POST.cluster_size)
        instance_type = bottle.request.POST.instance_type
        local_ssds = bottle.request.POST.local_ssds
        image = bottle.request.POST.image
        master_instance_type = bottle.request.POST.master_instance_type
        zone = bottle.request.POST.zone
        network = bottle.request.POST.network
        bucket = bottle.request.POST.bucket
        private_key = bottle.request.POST.private_key
        public_key = bottle.request.POST.public_key
        themis_config_directory = bottle.request.POST.themis_config_directory

        try:
            launch_google_cluster(
                cluster_name, cluster_size, instance_type, local_ssds, image,
                master_instance_type, network, zone, bucket, private_key,
                public_key, themis_config_directory, provider_info["google"],
                redis_client)
        except ProcessExecutionError as e:
            return display_error(e)

    elif bottle.request.POST.terminate:
        cluster_ID = int(bottle.request.POST.terminate)
        print "Terminating cluster %d" % cluster_ID

        try:
            terminate_cluster(cluster_ID, redis_client)
        except ProcessExecutionError as e:
            return display_error(e)

    elif bottle.request.POST.bring_online:
        cluster_ID = int(bottle.request.POST.bring_online)
        print "Bringing cluster %d online" % cluster_ID

        print "Fetching relevant information from redis..."
        # Fetch instance type from redis
        instance_type = redis_client.hget(
            "cluster:%d" % cluster_ID, "instance_type")
        themis_directory = redis_client.hget(
            "cluster:%d" % cluster_ID, "themis_directory")

        # Run configuration command on master via its external address.
        master = cluster_info[cluster_ID]["master"][1]
        print "Adding nodes to cluster..."
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            master, username=cluster_info[cluster_ID]["username"])

        # Fetch a recent list of online nodes.
        zone = cluster_info[cluster_ID]["zone"]
        provider = cluster_info[cluster_ID]["provider"]
        cluster_status = get_cluster_status(provider, cluster_ID, zone)
        node_list = cluster_status["online_slaves"]
        # Use internal addresses
        node_list = [node[0] for node in node_list]

        device_map = None
        if provider == "amazon":
            device_map = provider_info[provider]["device_map"]
        elif provider == "google":
            # We can look up the number of devices directly from redis.
            device_map = {}
            device_map[instance_type] = int(redis_client.hget(
                "cluster:%d" % cluster_ID, "local_ssds"))

        # Configure the master with cluster information
        command = "%s add" % os.path.join(
            themis_directory, "src/scripts/themis/cluster/configure_cluster.py")
        for node in node_list:
            command = "%s %s" % (command, node)

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

        # Format disks
        print "Formatting disks..."
        devices_are_partitions = ""
        # NVME devices on GCE show up as partitions rather than devices that
        # need to be partitioned with fdisk, so if using NVME,
        # devices_are_partitions should be set to the '--format_disks'
        # option. However, since the nvme debian image appears to be buggy,
        # we'll launch in SCSI mode, which does require the fdisk, so leave
        # this option string blank for both providers.
        command = "%s \"%s %s --format_disks\"" % (
            os.path.join(
                themis_directory,
                "src/scripts/themis/cluster/parallel_ssh.py"),
            os.path.join(
                themis_directory,
                "src/scripts/themis/cluster/mount_disks.py"),
            devices_are_partitions)

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

        # Set master hostname for slave nodes
        print "Setting master information on slaves..."
        command = "%s \"%s\"" % (
                      os.path.join(
                          themis_directory,
                          "src/scripts/themis/cluster/parallel_ssh.py"),
                      os.path.join(
                          themis_directory,
                          "src/scripts/themis/cloud/set_master_hostname.py"))

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

        # Build themis rc file so nodes can see master redis
        print "Building .themisrc files"
        command = "%s -m \"%s\"" % (
                      os.path.join(
                          themis_directory,
                          "src/scripts/themis/cluster/parallel_ssh.py"),
                      os.path.join(
                          themis_directory,
                          "src/scripts/themis/cluster/build_themis_rc.py"))

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

        # By default, populate with eth0 and all devices - half I/O and half
        # intermediate. If there are extra devices give them to the I/O
        print "Configuring cluster with default interface and disks..."
        interface = "eth0"
        num_devices = device_map[instance_type]
        disk_list = ["/mnt/disks/disk_%d" % x for x in xrange(num_devices)]
        if num_devices == 1:
            # Special case for 1 device, use for both input and output.
            io_disks = disk_list
            intermediate_disks = disk_list
        else:
            io_devices = num_devices / 2
            if num_devices % 2 > 0:
                # Give extra device to the I/O disks.
                io_devices += 1
            io_disks = disk_list[0:io_devices]
            intermediate_disks = disk_list[io_devices:]

        # Configure the cluster
        command = "%s interfaces %s" % (
            os.path.join(
                themis_directory,
                "src/scripts/themis/cluster/configure_cluster.py"),
            interface)

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

        if len(io_disks) > 0:
            command = "%s io_disks" % os.path.join(
                themis_directory, "src/scripts/themis/cluster/configure_cluster.py")
            for disk in io_disks:
                command = "%s %s" % (command, disk)

            channel = ssh_client.get_transport().open_session()
            channel.get_pty()
            channel.exec_command(command)
            return_code = channel.recv_exit_status()
            if return_code != 0:
                while channel.recv_stderr_ready():
                    stderr = channel.recv_stderr(1024)
                    sys.stderr.write(stderr)
                sys.exit(return_code)

        if len(intermediate_disks) > 0:
            command = "%s intermediate_disks" % os.path.join(
                themis_directory, "src/scripts/themis/cluster/configure_cluster.py")
            for disk in intermediate_disks:
                command = "%s %s" % (command, disk)

            channel = ssh_client.get_transport().open_session()
            channel.get_pty()
            channel.exec_command(command)
            return_code = channel.recv_exit_status()
            if return_code != 0:
                while channel.recv_stderr_ready():
                    stderr = channel.recv_stderr(1024)
                    sys.stderr.write(stderr)
                sys.exit(return_code)

        # Set cluster status to online.
        redis_client.hset("cluster:%d" % cluster_ID, "cluster_status", "Online")

    elif bottle.request.POST.persist_to_storage:
        cluster_ID = int(bottle.request.POST.persist_to_storage)
        provider = cluster_info[cluster_ID]["provider"]
        print "Persisting logs from cluster %d to cloud storage" % cluster_ID

        print "Fetching relevant information from redis..."
        bucket = redis_client.hget(
            "cluster:%d" % cluster_ID, "bucket")
        log_directory = redis_client.hget(
            "cluster:%d" % cluster_ID, "log_directory")
        themis_directory = redis_client.hget(
            "cluster:%d" % cluster_ID, "themis_directory")

        # Run configuration command on master via its external address.
        master = cluster_info[cluster_ID]["master"][1]
        print "Persisting logs to storage..."
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            master, username=cluster_info[cluster_ID]["username"])

        command = os.path.join(
            themis_directory, "src/scripts/themis/cloud/upload_logs.py")

        channel = ssh_client.get_transport().open_session()
        channel.get_pty()
        channel.exec_command(command)
        return_code = channel.recv_exit_status()
        if return_code != 0:
            while channel.recv_stderr_ready():
                stderr = channel.recv_stderr(1024)
                sys.stderr.write(stderr)
            sys.exit(return_code)

    return list_clusters()

@bottle.route('/', method='GET')
def list_clusters():
    global redis_client, jinja_env, cluster_info
    print "Listing clusters"

    clusters = redis_client.smembers("clusters")
    clusters = map(lambda x : int(x), clusters)
    cluster_info = {}
    for cluster in clusters:
        info = redis_client.hgetall("cluster:%s" % cluster)
        cluster_info[cluster] = info
        provider = info["provider"]
        zone = info["zone"]
        cluster_status = get_cluster_status(provider, cluster, zone)
        for key, value in cluster_status.iteritems():
            cluster_info[cluster][key] = value

        master_status = "Offline"
        master_connectible = False
        if cluster_status["num_online_masters"] == 1:
            master_status = "Online"
        else:
            if cluster_status["num_booting_masters"] == 1:
                master_status = "Booting"

        cluster_info[cluster]["master_status"] = master_status

        master = cluster_info[cluster]["master"]
        if master != None:
            # Use external address.
            cluster_info[cluster]["master_address"] = master[1]
        else:
            cluster_info[cluster]["master_address"] = None

    template = jinja_env.get_template("display_clusters.jinja2")
    return template.render(
        clusters = clusters, cluster_info = cluster_info, now = time.asctime())

@bottle.route('/new_amazon_cluster', method='GET')
def new_cluster():
    global redis_client, jinja_env, provider_info
    print "Showing new Amazon cluster form"
    subnets = provider_info["amazon"]["subnets"]
    placement_groups = provider_info["amazon"]["placement_groups"]
    security_groups = provider_info["amazon"]["security_groups"]
    HVM_images = provider_info["amazon"]["HVM_images"]
    PV_images = provider_info["amazon"]["PV_images"]
    buckets = provider_info["amazon"]["buckets"]
    instances = provider_info["amazon"]["instances"]
    vm_type_map = provider_info["amazon"]["vm_type_map"]
    placement_groups_map = provider_info["amazon"]["placement_groups_map"]
    ebs_optimized_map = provider_info["amazon"]["ebs_optimized_map"]

    template = jinja_env.get_template("new_amazon_cluster.jinja2")
    return template.render(
        subnets=subnets, placement_groups=placement_groups,
        security_groups=security_groups, HVM_images=HVM_images,
        PV_images=PV_images, buckets=buckets, instances=instances,
        vm_type_map=vm_type_map, placement_groups_map=placement_groups_map,
        ebs_optimized_map=ebs_optimized_map,
        default_config_dir=DEFAULT_CONFIG_DIR)

@bottle.route('/new_google_cluster', method='GET')
def new_cluster():
    global redis_client, jinja_env, provider_info
    print "Showing new Google cluster form"
    networks = provider_info["google"]["networks"]
    images = provider_info["google"]["images"]
    buckets = provider_info["google"]["buckets"]
    instances = provider_info["google"]["instances"]
    zones = provider_info["google"]["zones"]

    template = jinja_env.get_template("new_google_cluster.jinja2")
    return template.render(
        networks=networks, images=images, buckets=buckets,
        instances=instances, zones=zones, default_config_dir=DEFAULT_CONFIG_DIR)

def main():
    global provider_info, redis_client

    parser = argparse.ArgumentParser(
        description="Run a web-interface for provisioning cloud clusters")
    parser.add_argument("config", help="Cloud provider config file")

    utils.add_redis_params(parser)
    parser.add_argument(
        "--port", "-p", help="port on which the GUI accepts HTTP connections",
        type=int, default=4281)
    args = parser.parse_args()

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)
    # Test the connection to redis to fail early if redis isn't running.
    clusters = redis_client.smembers("clusters")

    # Perform Amazon configuration
    amazon_info = authenticate("amazon", args.config)
    if amazon_info != None:
        print "Fetching Amazon provider information..."
        provider_info["amazon"] = amazon_info

        aws = plumbum.local["aws"]

        # Fetch EC2 configuration information.
        # Since these commands take some time we'll run them in the background
        subnet_cmd = aws["--profile"]["themis"]["ec2"]["describe-subnets"] & BG
        placement_group_cmd =\
            aws["--profile"]["themis"]["ec2"]["describe-placement-groups"] & BG
        security_group_cmd =\
            aws["--profile"]["themis"]["ec2"]["describe-security-groups"] & BG
        AMI_cmd =\
            aws["--profile"]["themis"]["ec2"]["describe-images"]\
            ["--owners"]["self"] & BG
        S3_cmd = aws["--profile"]["themis"]["s3api"]["list-buckets"] & BG

        print "Gathering information for subnets..."
        stdout = wait_on_background_command(subnet_cmd)

        result = json.loads(stdout)
        subnets = result["Subnets"]
        subnets = [(x["SubnetId"], x["AvailabilityZone"]) for x in subnets]
        provider_info["amazon"]["subnets"] = subnets

        print "Gathering information for placement groups..."
        stdout = wait_on_background_command(placement_group_cmd)

        result = json.loads(stdout)
        placement_groups = result["PlacementGroups"]
        placement_groups = [x["GroupName"] for x in placement_groups]
        provider_info["amazon"]["placement_groups"] = placement_groups

        print "Gathering information for security groups..."
        stdout = wait_on_background_command(security_group_cmd)

        result = json.loads(stdout)
        security_groups = result["SecurityGroups"]
        security_groups = [(x["GroupName"], x["GroupId"]) for x in security_groups]
        provider_info["amazon"]["security_groups"] = security_groups

        print "Gathering information for AMIs..."
        stdout = wait_on_background_command(AMI_cmd)

        result = json.loads(stdout)
        images = result["Images"]
        HVM_images = [(x["Name"], x["ImageId"]) for x in images \
                      if x["VirtualizationType"] == "hvm"]
        PV_images = [(x["Name"], x["ImageId"]) for x in images \
                      if x["VirtualizationType"] == "paravirtual"]
        provider_info["amazon"]["HVM_images"] = HVM_images
        provider_info["amazon"]["PV_images"] = PV_images

        print "Gathering information for S3 buckets..."
        stdout = wait_on_background_command(S3_cmd)

        result = json.loads(stdout)
        buckets = result["Buckets"]
        buckets = [x["Name"] for x in buckets]
        provider_info["amazon"]["buckets"] = buckets

        # Load instance type and device information
        print "Gathering information for instance types..."
        parser = ConfigParser.SafeConfigParser()
        parser.read(INSTANCE_TYPE_CONFIG)

        device_map = {}
        instances = []
        for instance_type, num_devices in parser.items("devices"):
            device_map[instance_type] = int(num_devices)
            instances.append(instance_type)
        provider_info["amazon"]["instances"] = instances
        provider_info["amazon"]["device_map"] = device_map

        vm_type_map = {}
        for instance_type, vm_type in parser.items("vm_type"):
            vm_type_map[instance_type] = vm_type
        provider_info["amazon"]["vm_type_map"] = vm_type_map

        placement_groups_map = {}
        for instance_type, placement_groups_enabled in parser.items("placement_groups"):
            placement_groups_map[instance_type] = placement_groups_enabled
        provider_info["amazon"]["placement_groups_map"] = placement_groups_map

        ebs_optimized_map = {}
        for instance_type, ebs_optimized in parser.items("EBS_optimized"):
            ebs_optimized_map[instance_type] = ebs_optimized
        provider_info["amazon"]["ebs_optimized_map"] = ebs_optimized_map

    # Perform Google configuration
    google_info = authenticate("google", args.config)
    if google_info != None:
        print "Fetching Google provider information..."
        provider_info["google"] = google_info

        gcloud = plumbum.local["gcloud"]
        gsutil = plumbum.local["gsutil"]

        # Get list of zones
        print "Retrieving zone information..."
        zones = gcloud["compute"]["zones"]["list"]\
                ["--format"]["json"]()
        zones = json.loads(zones)
        zones = [x["name"] for x in zones]
        if len(zones) == 0:
            print >>sys.stderr, "Found no zones"
            sys.exit(1)
        provider_info["google"]["zones"] = zones

        print "Retrieving network information..."
        networks = gcloud["compute"]["networks"]["list"]["--format"]["json"]()
        networks = json.loads(networks)
        networks = [x["name"] for x in networks]
        provider_info["google"]["networks"] = networks

        print "Retrieving image information"
        images = gcloud["compute"]["images"]["list"]["--no-standard-images"]\
                 ["--format"]["json"]()
        images = json.loads(images)
        images = [x["name"] for x in images]
        provider_info["google"]["images"] = images

        print "Retrieving storage bucket information"
        buckets = gsutil["ls"]()
        buckets = buckets.split("\n")
        buckets = [bucket for bucket in buckets if len(bucket) > 0]
        buckets = [bucket.split("gs://")[1].split("/")[0] for bucket in buckets]
        provider_info["google"]["buckets"] = buckets

        print "Retrieving instance type information"
        instances = gcloud["compute"]["machine-types"]["list"]\
                    ["--format"]["json"]()
        instances = json.loads(instances)
        instances = [x["name"] for x in instances]
        instances = list(set(instances))
        instances.sort()
        provider_info["google"]["instances"] = instances
    try:
        bottle.run(host='0.0.0.0', port=args.port)
    except socket.error, e:
        print e
        # Return error 42 to indicate that we can't bind, so that scripts
        # calling this one can handle that case specially
        return constants.CANNOT_BIND

if __name__ == "__main__":
    sys.exit(main())
