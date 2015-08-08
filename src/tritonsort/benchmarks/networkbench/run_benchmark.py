#!/usr/bin/env python

import os, sys, argparse

BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))

THEMIS_SCRIPTS_DIR = os.path.join(
    BENCHMARK_DIR, os.pardir, os.pardir, os.pardir, "scripts", "themis")

CLUSTER_SCRIPTS_DIR = os.path.join(THEMIS_SCRIPTS_DIR, "cluster")

# Prepend so we can get the other run_benchmark module
sys.path.insert(0, THEMIS_SCRIPTS_DIR)

from run_benchmark import run_benchmark_iterations
import utils

sys.path.append(CLUSTER_SCRIPTS_DIR)

from conf_utils import read_conf_file

def main():
    log_directory = read_conf_file("cluster.conf", "cluster", "log_directory")
    log_directory = os.path.expanduser(log_directory)
    log_directory = os.path.join(log_directory, "networkbench")

    parser = argparse.ArgumentParser(
        description="Harness for network benchmark application")
    parser.add_argument(
        "--config", "-c", help="config file to use for the benchmark "
        "(default: %(default)s)",
        default=os.path.join(BENCHMARK_DIR, "config.yaml"), type=str)
    parser.add_argument(
        "--log_directory", "-l",
        help="directory containing logs for an experiment "
        "(default: %(default)s)",
        default=log_directory)
    parser.add_argument(
        "--profiler", help="path to the binary of a profiling tool to use, for "
        "example valgrind or operf")
    parser.add_argument(
        "--profiler_options", help="options surrounded by quotes to pass to "
        "the profiler", type=str, default="")
    parser.add_argument(
        "--iterations", "-i", help="run the benchmark this many times "
        "(default: %(default)s)", type=int, default=1)
    parser.add_argument(
        "--sleep", "-s", help="sleep this many seconds between iterations "
        "(default: %(default)s)", type=int, default=0)
    parser.add_argument(
        "--per_peer_config", help="use separate config files for each peer, by "
        "appending the peer's IP address to the config file name: .A.B.C.D",
        action="store_true", default=False)
    parser.add_argument(
        "--dump_core_directory", "-d", help="dump core file to this directory "
        "if the benchmark crashes", default=None)
    parser.add_argument(
        "peer_ips", help="comma delimited list of host IPs to use for "
        "benchmarking")
    parser.add_argument(
        "--remote_connections_only", "-r", help="Only send to remote peers, "
        "instead of sending all-to-all, which includes localhost",
        action="store_true", default=False)

    utils.add_interfaces_params(parser)

    args = parser.parse_args()
    binary = os.path.join(BENCHMARK_DIR, "networkbench")
    delete_output = False
    solo_mode = False
    stage_stats = "sender,receiver"

    params = "-REMOTE_CONNECTIONS_ONLY %d" % (args.remote_connections_only)

    run_benchmark_iterations(
        binary, args.log_directory, args.config, args.peer_ips, args.profiler,
        args.profiler_options, args.iterations, args.sleep, delete_output,
        args.per_peer_config, args.dump_core_directory, solo_mode,
        stage_stats, args.interfaces, params)

if __name__ == "__main__":
    sys.exit(main())
