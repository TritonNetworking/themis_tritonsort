#!/usr/bin/env python

import argparse, getpass, os, paramiko, socket, sys, redis
from parallel_ssh import parallel_ssh

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(SCRIPT_DIR_PATH, os.pardir, os.pardir))

from themis import utils

def set_ip_addresses(redis_client, nodes, interface_names, username):
    # Given a list of nodes, ssh into each and get the IP address corresponding
    # to the selected interfaces.
    ips = {}
    return_code = 0
    for node in nodes:
        ips[node] = []
    for interface in interface_names:

        # Get the IP address for this interface to be used in Themis
        command = "/sbin/ifconfig %s | sed \'/inet addr/!d;s/.*addr:\(.*\) "\
            "Bcast.*$/\\1/\'" % interface

        return_code, stdout_ifconfig, stderr = parallel_ssh(
            redis_client, command, username, nodes, ignore_bad_hosts=True,
            master=False, verbose=False)
        if return_code != 0:
            print >>sys.stderr, "Failed parallel ssh of command '%s'" % command
            break

        for node in nodes:
            # Get IP address of this interface from stdout
            # Make sure the command actually succeeded
            ips[node].append(stdout_ifconfig[node].strip())

    # Get internal/external IP and hostname for cluster display purposes.
    command = "/bin/grep internal_dns ~/node.conf | awk -F= \'{print $2}\' | tr -d \" \""

    return_code, stdout_internal_dns, stderr = parallel_ssh(
        redis_client, command, username, nodes, ignore_bad_hosts=True,
        master=False, verbose=False)
    if return_code != 0:
        print >>sys.stderr, "Failed parallel ssh of command '%s'" % command

    command = "/bin/grep external_dns ~/node.conf | awk -F= \'{print $2}\' | tr -d \" \""

    return_code, stdout_external_dns, stderr = parallel_ssh(
        redis_client, command, username, nodes, ignore_bad_hosts=True,
        master=False, verbose=False)
    if return_code != 0:
        print >>sys.stderr, "Failed parallel ssh of command '%s'" % command

    command = "/bin/grep internal_ip ~/node.conf | awk -F= \'{print $2}\' | tr -d \" \""

    return_code, stdout_internal_ip, stderr = parallel_ssh(
        redis_client, command, username, nodes, ignore_bad_hosts=True,
        master=False, verbose=False)
    if return_code != 0:
        print >>sys.stderr, "Failed parallel ssh of command '%s'" % command

    command = "/bin/grep external_ip ~/node.conf | awk -F= \'{print $2}\' | tr -d \" \""

    return_code, stdout_external_ip, stderr = parallel_ssh(
        redis_client, command, username, nodes, ignore_bad_hosts=True,
        master=False, verbose=False)
    if return_code != 0:
        print >>sys.stderr, "Failed parallel ssh of command '%s'" % command

    for node in nodes:
        # hostname and ipv4_address maps are used for file locations, so just
        # use the first interface
        if len(ips[node]) > 0:
            redis_client.hset("ipv4_address", node, ips[node][0])
        # Add a reverse hostname map
        for ip in ips[node]:
            redis_client.hset("hostname", ip, node)

        # Add external/internal dns/ip for cluster monitor
        redis_client.hset(
            "internal_dns", node, stdout_internal_dns[node].strip())
        redis_client.hset(
            "external_dns", node, stdout_external_dns[node].strip())
        redis_client.hset(
            "internal_ip", node, stdout_internal_ip[node].strip())
        redis_client.hset(
            "external_ip", node, stdout_external_ip[node].strip())

        # Add all IP addresses to the interfaces map
        redis_client.hset("interfaces", node, ",".join(ips[node]))

    return return_code

def set_disk_lists(redis_client, nodes, io_disks, intermediate_disks):
    # Even though we only support the case where all nodes have the same
    # disk paths, the existing API has a separate redis key for each node,
    # so just go with that.
    printed_io_warning = False
    printed_intermediate_warning = False
    for node in nodes:
        io_key = "node_io_disks:%s" % node
        intermediate_key = "node_local_disks:%s" % node
        if io_disks != None:
            if len(io_disks) > 0:
                redis_client.delete(io_key)
                redis_client.sadd(io_key, *io_disks)
            elif not printed_io_warning:
                printed_io_warning = True
                print "No io disk list set. Use 'io_disks' command to set io " \
                    "disks."

        if intermediate_disks != None:
            if len(intermediate_disks) > 0:
                redis_client.delete(intermediate_key)
                redis_client.sadd(intermediate_key, *intermediate_disks)
            elif not printed_intermediate_warning:
                printed_intermediate_warning = True
                print "No intermediate disk list set. Use " \
                    "'intermediate_disks' command to set intermediate disks."

def remove_disk_lists(redis_client, nodes):
    for node in nodes:
        io_key = "node_io_disks:%s" % node
        intermediate_key = "node_local_disks:%s" % node
        redis_client.delete(io_key)
        redis_client.delete(intermediate_key)

def add_nodes(redis_client, nodes, username):
    nodes = filter(lambda x : len(x) > 0, nodes)
    # Get the full hostname for each node
    nodes = [socket.getfqdn(node) for node in nodes]

    print "Adding %s" % (nodes)
    num_added = redis_client.sadd("nodes", *nodes)
    print "Added %d nodes" % (num_added)

    # If we have already set interface information, then go fetch IP addresses
    # for these new nodes.
    interfaces = redis_client.lrange("interface_names", 0, -1)
    if len(interfaces) > 0:
        return_code = set_ip_addresses(
            redis_client, nodes, interfaces, username)
        if return_code != 0:
            return return_code
    else:
        print "No interface information set. Use 'interfaces' command to set " \
            "interfaces and fetch IP addresses."

    # If we already set disk lists, set the keys in redis for these nodes
    io_disks = redis_client.smembers("io_disks")
    intermediate_disks = redis_client.smembers("intermediate_disks")
    set_disk_lists(redis_client, nodes, io_disks, intermediate_disks)

    return 0

def remove_nodes(redis_client, nodes):
    nodes = filter(lambda x : len(x) > 0, nodes)
    # Get the full hostname for each node
    nodes = [socket.getfqdn(node) for node in nodes]

    print "Removing %s" % nodes
    num_removed = redis_client.srem("nodes", *nodes)

    for node in nodes:
        redis_client.hdel("ipv4_address", node)
        redis_client.hdel("hostname", node)
        redis_client.hdel("interfaces", node)

    remove_disk_lists(redis_client, nodes)

    print "Removed %d nodes" % (num_removed)

    return 0

def clear_nodes(redis_client):
    print "Removing all nodes and clearing interface/disk settings"
    nodes = redis_client.smembers("nodes")
    remove_disk_lists(redis_client, nodes)

    redis_client.delete("nodes")
    redis_client.delete("interface_names")

    redis_client.delete("ipv4_address")
    redis_client.delete("hostname")
    redis_client.delete("interfaces")
    redis_client.delete("num_interfaces")

    redis_client.delete("io_disks")
    redis_client.delete("intermediate_disks")

    return 0

def set_io_disks(redis_client, disks):
    disks = filter(lambda x : len(x) > 0, disks)
    redis_client.delete("io_disks")
    if len(disks) > 0:
        redis_client.sadd("io_disks", *disks)

    nodes = redis_client.smembers("nodes")
    if len(nodes) > 0:
        set_disk_lists(redis_client, nodes, disks, None)
    else:
        print "No nodes in cluster. Use 'add' command to add nodes and set " \
            "io disks "

    return 0

def set_intermediate_disks(redis_client, disks):
    disks = filter(lambda x : len(x) > 0, disks)
    redis_client.delete("intermediate_disks")
    if len(disks) > 0:
        redis_client.sadd("intermediate_disks", *disks)

    nodes = redis_client.smembers("nodes")
    if len(nodes) > 0:
        set_disk_lists(redis_client, nodes, None, disks)
    else:
        print "No nodes in cluster. Use 'add' command to add nodes and set " \
            "intermediate disks "

    return 0


def set_interfaces(redis_client, interfaces, username):
    interfaces = filter(lambda x : len(x) > 0, interfaces)
    # Set the list of interface names
    # Use a list instead of a set so we get order
    # However, we still need to enforce uniqueness
    seen = set([])
    interface_list = []
    for interface in interfaces:
        if interface not in seen:
            interface_list.append(interface)
            seen.add(interface)
    interfaces = interface_list

    redis_client.delete("interface_names")
    redis_client.delete("ipv4_address")
    redis_client.delete("hostname")
    redis_client.delete("interfaces")
    redis_client.delete("num_interfaces")

    if len(interfaces) > 0:
        redis_client.rpush("interface_names", *interfaces)
    print "Using interfaces %s" % (interfaces)

    redis_client.set("num_interfaces", len(interfaces))

    nodes = redis_client.smembers("nodes")
    if len(nodes) > 0:
        return_code = set_ip_addresses(
            redis_client, nodes, interfaces, username)
        if return_code != 0:
            return return_code
    else:
        print "No nodes in cluster. Use 'add' command to add nodes and fetch " \
            "IP addresses "

    return 0

def main():
    usage_string = """<command> [args]

configure_cluster.py add NODE_LIST
  add nodes to the cluster

configure_cluster.py remove NODE_LIST
  remove nodes from the cluster

configure_cluster.py clear
  remove ALL nodes from the cluster

configure_cluster.py io_disks DISK_LIST
  set disks for input/output files

configure_cluster.py intermediate_disks DISK_LIST
  set disks for intermediate files

configure_cluster.py interfaces INTERFACE_LIST
  set network interfaces for inter-node communication

Argument lists are SPACE-DELIMITED.
"""
    parser = argparse.ArgumentParser(
        usage=usage_string)
    utils.add_redis_params(parser)

    parser.add_argument("command", help="Command to issue: add, remove, clear, "
                        "io_disks, intermediate_disks")
    parser.add_argument(
        "arguments", nargs="*", help="Command specific arguments.")
    parser.add_argument(
        "--user", help="the username to run under "
        "(default: %(default)s)", default=getpass.getuser())
    utils.add_interfaces_params(parser)

    args = parser.parse_args()
    command = args.command
    arguments = args.arguments
    username = args.user

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    valid_commands = [
        "add", "remove", "clear", "io_disks", "intermediate_disks",
        "interfaces"]
    if command not in valid_commands:
        print "Invalid command %s" % command
        print "usage:"
        print usage_string
        return 1

    # Check that clear has no arguments and all other argument lists are
    # non-empty
    if command == "clear" and len(arguments) > 0:
        print "Command clear does not take arguments"
        print "usage:"
        print usage_string
        return 1
    if command != "clear" and len(arguments) == 0:
        print "Command %s requires at least 1 argument" % command
        print "usage:"
        print usage_string
        return 1

    if command == "add":
        return add_nodes(redis_client, arguments, username)
    elif command == "remove":
        return remove_nodes(redis_client, arguments)
    elif command == "clear":
        return clear_nodes(redis_client)
    elif command == "io_disks":
        return set_io_disks(redis_client, arguments)
    elif command == "intermediate_disks":
        return set_intermediate_disks(redis_client, arguments)
    else:
        assert command == "interfaces"
        return set_interfaces(redis_client, arguments, username)


if __name__ == "__main__":
    sys.exit(main())
