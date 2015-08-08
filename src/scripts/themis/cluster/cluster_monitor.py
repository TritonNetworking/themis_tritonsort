#!/usr/bin/env python

import sys, argparse, bottle, getpass, jinja2, os, time, socket, re
from plumbum.cmd import awk, fping, pkill, rm
from plumbum import BG
import configure_cluster
from parallel_ssh import parallel_ssh

from conf_utils import read_conf_file

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

from plumbum import local
parallel_ssh_cmd = local[os.path.join(SCRIPT_DIR, "parallel_ssh.py")]

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, "job_runner")))

import redis_utils

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir)))

import constants
from common import unitconversion

coordinator_db = None
username = None

disk_mountpoint = read_conf_file("cluster.conf", "cluster", "disk_mountpoint")
username = read_conf_file("cluster.conf", "cluster", "username")
themis_directory = read_conf_file("cluster.conf", "cluster", "themis_directory")
# Display the master's external address on the status page.
master_address = read_conf_file(
    "cluster.conf", "cluster", "master_external_address")

MOUNT_SCRIPT = os.path.join(
    themis_directory, "src", "scripts", "themis", "cluster", "mount_disks.py")
UPDATE_SCRIPT = os.path.join(
    themis_directory, "src", "scripts", "themis", "cluster", "update_repo.py")

generate_command = None
generate_data_size = None

def datetimeformat(value, format='%m-%d-%Y %H:%M:%S'):
    return time.strftime(format, time.localtime(float(value)))

def dfs_lookup(path):
    global coordinator_db, jinja_env, disk_mountpoint, username
    if path == None:
        path = disk_mountpoint

    # Make path canonical
    if path[0] != "/":
        path = "/%s" % path

    print path
    path = os.path.expanduser(path)
    print path
    path = os.path.realpath(path)

    if path[len(path) - 1] != "/":
        path = "%s/" % path

    print "DFS lookup for %s" % path

    # Make sure path is within our valid disk directory
    if path[0:len(disk_mountpoint)] != disk_mountpoint:
        print "Illegal prefix for path %s" % path
        return

    return_code, stdout, stderr = parallel_ssh(
        coordinator_db.redis_client,
        "find %s -maxdepth 1 -mindepth 1 -printf \"%%f,%%y,%%s\\n\"" % path,
        username, None, False, False)

    directories = set()
    files = []
    total_size = 0
    for host, results in stdout.iteritems():
        results = results.split("\r\n")
        results = [x for x in results if len(x) > 0]

        for result in results:
            file_name, file_type, file_size = tuple(result.split(","))
            if file_type == "d":
                directories.add(file_name)
            elif file_type == "f":
                files.append((file_name, file_size, host))
                total_size += int(file_size)

    diretories = list(directories)
    directories = sorted(directories)
    files = sorted(files, key=lambda x : x[0])

    # Add a wildcard for disks so we can explore across all disks
    if path == disk_mountpoint:
        directories.append("disk_*")


    return (directories, files, total_size, path)

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.abspath(__file__))), trim_blocks=True)

jinja_env.filters["datetime"] = datetimeformat

@bottle.route('/', method='POST')
def modify_cluster():
    global coordinator_db, username
    print "Modifying Cluster"

    if bottle.request.POST.add:
        print "Adding nodes"
        nodes_to_add = bottle.request.POST.nodes_to_add
        # We accept space delimited, comma delimited, or both
        nodes_to_add = re.split(",| |, ", nodes_to_add)
        nodes_to_add = filter(lambda x : len(x) > 0, nodes_to_add)

        if len(nodes_to_add) > 0:
            configure_cluster.add_nodes(
                coordinator_db.redis_client, nodes_to_add, username)

    elif bottle.request.POST.remove:
        allowed = bottle.request.POST.getall('allow_remove_and_clear')
        if len(allowed) == 1 and allowed[0] == "True":
            print "Removing nodes"
            # Remove nodes that the user clicked.
            nodes_to_remove = bottle.request.POST.getall('selected')

            if len(nodes_to_remove) > 0:
                configure_cluster.remove_nodes(
                    coordinator_db.redis_client, nodes_to_remove)
    elif bottle.request.POST.wipe:
        allowed = bottle.request.POST.getall('allow_remove_and_clear')
        if len(allowed) == 1 and allowed[0] == "True":
            print "Wiping cluster"
            configure_cluster.clear_nodes(coordinator_db.redis_client)
    elif bottle.request.POST.configure:
        print "Adding nodes"
        nodes_to_add = bottle.request.POST.nodes_to_add
        # We accept space delimited, comma delimited, or both
        nodes_to_add = re.split(",| |, ", nodes_to_add)
        nodes_to_add = filter(lambda x : len(x) > 0, nodes_to_add)

        if len(nodes_to_add) > 0:
            configure_cluster.add_nodes(
                coordinator_db.redis_client, nodes_to_add, username)

        print "Updating disk lists"
        # We accept space delimited, comma delimited, or both
        io_disks = bottle.request.POST.io_disks
        io_disks = re.split(",| |, ", io_disks)
        print io_disks
        configure_cluster.set_io_disks(coordinator_db.redis_client, io_disks)
        intermediate_disks = bottle.request.POST.intermediate_disks
        intermediate_disks = re.split(",| |, ", intermediate_disks)
        print intermediate_disks
        configure_cluster.set_intermediate_disks(
            coordinator_db.redis_client, intermediate_disks)

        print "Updating interfaces"
        # We accept space delimited, comma delimited, or both
        interfaces = bottle.request.POST.interfaces
        interfaces = re.split(",| |, ", interfaces)
        print interfaces
        configure_cluster.set_interfaces(
            coordinator_db.redis_client, interfaces, username)

    # Call the normal / method.
    return cluster_status()

@bottle.route('/', method='GET')
def cluster_status():
    global coordinator_db, jinja_env, disk_mountpoint, master_address
    print "Displaying cluster status"

    cluster_info = {}

    nodes = coordinator_db.known_nodes
    for node in nodes:
        cluster_info[node] = {}
        cluster_info[node]["status"] = "Dead"

    # Check liveness of all nodes by pinging.
    command = fping["-a"]
    for node in nodes:
        command = command[node]
    command = command | awk['{print $1}']
    live_nodes = command()
    live_nodes = live_nodes.encode("ascii")
    live_nodes = live_nodes.strip().split("\n")
    live_nodes = filter(lambda x : len(x) > 0, live_nodes)

    internal_ips = []

    for node in live_nodes:
        cluster_info[node]["status"] = "Alive"
        cluster_info[node]["internal_dns"] = coordinator_db.redis_client.hget(
            "internal_dns", node)
        cluster_info[node]["external_dns"] = coordinator_db.redis_client.hget(
            "external_dns", node)
        cluster_info[node]["internal_ip"] = coordinator_db.redis_client.hget(
            "internal_ip", node)
        cluster_info[node]["external_ip"] = coordinator_db.redis_client.hget(
            "external_ip", node)
        internal_ips.append(cluster_info[node]["internal_ip"])

    internal_ips = ",".join(internal_ips)

    io_disks = coordinator_db.redis_client.smembers("io_disks")
    io_disks = ", ".join(io_disks)
    intermediate_disks = coordinator_db.redis_client.smembers(
        "intermediate_disks")
    intermediate_disks = ", ".join(intermediate_disks)

    interfaces = coordinator_db.redis_client.lrange("interface_names", 0, 1)
    interfaces_list = interfaces
    interfaces = ", ".join(interfaces)
    print interfaces

    interface_addresses = coordinator_db.redis_client.hgetall("interfaces")
    for node in interface_addresses:
        # Separate comma delimited lists
        interface_addresses[node] = interface_addresses[node].split(",")


    template = jinja_env.get_template("cluster_status.jinja2")
    return template.render(
        cluster_info = cluster_info, io_disks = io_disks,
        intermediate_disks = intermediate_disks, interfaces = interfaces,
        interfaces_list = interfaces_list,
        interface_addresses = interface_addresses,
        internal_ips = internal_ips,
        master_address=master_address, now = time.asctime())

@bottle.route('/configure', method='GET')
def configure():
    global coordinator_db, jinja_env
    print "Displaying configuration page"

    io_disks = coordinator_db.redis_client.smembers("io_disks")
    io_disks = ", ".join(io_disks)
    intermediate_disks = coordinator_db.redis_client.smembers(
        "intermediate_disks")
    intermediate_disks = ", ".join(intermediate_disks)

    interfaces = coordinator_db.redis_client.lrange("interface_names", 0, 1)
    interfaces_list = interfaces
    interfaces = ", ".join(interfaces)
    print interfaces


    template = jinja_env.get_template("configure_cluster.jinja2")
    return template.render(
        io_disks = io_disks, intermediate_disks = intermediate_disks,
        interfaces = interfaces)

@bottle.route('/management', method='GET')
def management(updated=False):
    global coordinator_db, jinja_env
    print "Displaying cluster management page"

    template = jinja_env.get_template("management.jinja2")
    return template.render(updated=updated)

@bottle.route('/management', method='POST')
def management_post():
    global coordinator_db, username
    print "Management post"

    if bottle.request.POST.kill_themis or bottle.request.POST.remount_disks or \
            bottle.request.POST.format_disks:
        return_code, stdout, stderr = parallel_ssh(
            coordinator_db.redis_client,
            "pkill -f mapreduce\|cluster_coordinator.py\|node_coordinator.py\|"\
                "resource_monitor_gui.py\|job_status.py",
            username, None, False, True)

    if bottle.request.POST.remount_disks:
        return_code, stdout, stderr = parallel_ssh(
            coordinator_db.redis_client, MOUNT_SCRIPT,
            username, None, False, False)

    if bottle.request.POST.format_disks:
        return_code, stdout, stderr = parallel_ssh(
            coordinator_db.redis_client, "%s --format_disks" % MOUNT_SCRIPT,
            username, None, False, False)

    updated = False
    if bottle.request.POST.update_repo:
        recompile = bottle.request.POST.getall('recompile')
        recompile_option = ""
        if len(recompile) == 1 and recompile[0] == "True":
            recompile_option = "-r"

        branch = bottle.request.POST.branch
        return_code, stdout, stderr = parallel_ssh(
            coordinator_db.redis_client, "%s -b %s %s" % (
                UPDATE_SCRIPT, branch, recompile_option),
            username, None, False, True)

        updated = True

    return management(updated=updated)


@bottle.route('/data_generation', method='POST')
def data_generation_post():
    global coordinator_db, username, parallel_ssh_cmd, generate_command,\
        generate_data_size
    print "Data generation post"

    if bottle.request.POST.generate_data:
        print "Generating data"
        data_size = int(bottle.request.POST.data_size)
        unit = bottle.request.POST.unit
        print "Generating %d%s of data..." % (data_size, unit)
        files_per_disk = int(bottle.request.POST.files_per_disk)

        generate_data_size = unitconversion.convert(data_size, unit, "B")

        generate_command = parallel_ssh_cmd[
            "%s --no_sudo -g -n%d %d%s gensort_2013" % (
                os.path.join(SCRIPT_DIR, os.pardir,
                             "generate_graysort_inputs.py"),
                files_per_disk, data_size, unit)] & BG

    elif bottle.request.POST.abort_data_generation:
        print "Aborting data generation"

        pkill["-f"]["generate_graysort_inputs.py"]()
        generate_command = None

    elif bottle.request.POST.wipe_input:
        allowed = bottle.request.POST.getall('allow_wipe_data')
        if len(allowed) == 1 and allowed[0] == "True":
            print "Wiping input data"
            parallel_ssh_cmd[
                "%s input %s" % (
                    os.path.join(SCRIPT_DIR, os.pardir,
                                 "clear_disks.sh"), disk_mountpoint)]()

    elif bottle.request.POST.wipe_intermediate_output:
        allowed = bottle.request.POST.getall('allow_wipe_data')
        if len(allowed) == 1 and allowed[0] == "True":
            print "Wiping intermediate/output data"
            parallel_ssh_cmd[
                "%s noinput %s" % (
                    os.path.join(SCRIPT_DIR, os.pardir,
                                 "clear_disks.sh"), disk_mountpoint)]()

    elif bottle.request.POST.wipe_all:
        allowed = bottle.request.POST.getall('allow_wipe_data')
        if len(allowed) == 1 and allowed[0] == "True":
            print "Wiping all data"
            parallel_ssh_cmd[
                "%s all %s" % (
                    os.path.join(SCRIPT_DIR, os.pardir,
                                 "clear_disks.sh"), disk_mountpoint)]()


    # Call the normal /data_generation method.
    return data_generation()

@bottle.route('/data_generation', method='GET')
def data_generation():
    global jinja_env, generate_command, generate_data_size
    print "Displaying data generation page"

    # Use parallel ssh to check progress
    try:
        directories, files, total_size, path = dfs_lookup(os.path.join(
                disk_mountpoint, "disk_*", username, "inputs", "Graysort"))
    except ValueError:
        total_size = 0

    waiting = False
    if generate_command != None:
        if generate_command.ready():
            generate_command.wait()
            generate_command = None
        else:
            template = jinja_env.get_template(
                "data_generation_in_progress.jinja2")
            return template.render(
                current_size="%d" % total_size,
                total_size="%d" % generate_data_size,
                percent="%.2f" % (total_size * 100.0 / generate_data_size))

    # Not waiting on a command
    template = jinja_env.get_template("data_generation.jinja2")
    return template.render(input_size="%d" % total_size)

@bottle.route('/dfs_manager', method='GET')
def dfs_manager_no_path():
    return dfs_manager(None)

@bottle.route('/dfs_manager/', method='GET')
def dfs_manager_no_path():
    return dfs_manager(None)

@bottle.route('/dfs_manager/<path:path>', method='GET')
def dfs_manager(path):
    global coordinator_db, jinja_env, disk_mountpoint

    print "Displaying DFS manager page for %s" % path

    directories, files, total_size, path = dfs_lookup(path)

    template = jinja_env.get_template("dfs_manager.jinja2")
    return template.render(path=path, url_path=path[1:len(path)-1], directories=directories, files=files, total_size=total_size)

def main():
    global coordinator_db, username

    parser = argparse.ArgumentParser(
        description="Run a web-interface for monitoring the cluster.")
    utils.add_redis_params(parser)
    parser.add_argument(
        "--port", "-p", help="port on which the GUI accepts HTTP connections",
        type=int, default=4280)
    parser.add_argument(
        "--user", help="the username to run under "
        "(default: %(default)s)", default=getpass.getuser())
    args = parser.parse_args()

    coordinator_db = redis_utils.CoordinatorDB(
        args.redis_host, args.redis_port, args.redis_db)
    username = args.user

    try:
        bottle.run(host='0.0.0.0', port=args.port)
    except socket.error, e:
        print e
        # Return error 42 to indicate that we can't bind, so that scripts
        # calling this one can handle that case specially
        return constants.CANNOT_BIND

if __name__ == "__main__":
    sys.exit(main())
