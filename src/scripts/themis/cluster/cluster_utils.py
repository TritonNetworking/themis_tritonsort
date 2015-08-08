#!/usr/bin/env python

import sys, os, argparse
from plumbum.cmd import awk, fping

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, "job_runner")))

import redis_utils

def cluster_utils(command, coordinator_db):
    nodes = coordinator_db.known_nodes

    if command == "live":
        shell_command = fping["-a"]
        for node in nodes:
            shell_command = shell_command[node]
        shell_command = shell_command | awk['{print $1}']
        live_nodes = shell_command()
        live_nodes = live_nodes.encode("ascii")
        live_nodes = live_nodes.strip().split("\n")
        live_nodes = filter(lambda x : len(x) > 0, live_nodes)

        nodes = live_nodes

    internal_ips = []
    for node in nodes:
        internal_ips.append(coordinator_db.redis_client.hget("internal_ip", node))
    internal_ips = ",".join(internal_ips)

    print internal_ips

def main():

    parser = argparse.ArgumentParser(
        description="Cluster utility program for getting IP addresses")
    utils.add_redis_params(parser)
    parser.add_argument(
        "command", help="Utility command. Valid commands: all, live")
    args = parser.parse_args()

    coordinator_db = redis_utils.CoordinatorDB(
        args.redis_host, args.redis_port, args.redis_db)

    assert args.command in ["all", "live"]

    return cluster_utils(args.command, coordinator_db)

if __name__ == "__main__":
    sys.exit(main())
