#!/usr/bin/env python

import os, sys, argparse, yaml

BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))

THEMIS_SCRIPTS_DIR = os.path.join(
    BENCHMARK_DIR, os.pardir, os.pardir, os.pardir, "scripts", "themis")

CLUSTER_SCRIPTS_DIR = os.path.join(THEMIS_SCRIPTS_DIR, "cluster")

# Prepend so we can get the other run_benchmark module
sys.path.insert(0, THEMIS_SCRIPTS_DIR)

from run_benchmark import run_benchmark_iterations

sys.path.append(CLUSTER_SCRIPTS_DIR)

from conf_utils import read_conf_file

def main():
    log_directory = read_conf_file("cluster.conf", "cluster", "log_directory")
    log_directory = os.path.expanduser(log_directory)
    log_directory = os.path.join(log_directory, "storagebench")

    parser = argparse.ArgumentParser(
        description="Harness for storage benchmark application")
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
        "--delete_output", help="delete output files after run completes",
        action="store_true", default=False)
    parser.add_argument(
        "--per_peer_config", help="use separate config files for each peer, by "
        "appending the peer's IP address to the config file name: .A.B.C.D",
        action="store_true", default=False)
    parser.add_argument(
        "--dump_core_directory", "-d", help="dump core file to this directory "
        "if the benchmark crashes", default=None)
    parser.add_argument(
        "--read_only", "-r", help="Only read files, don't write",
        action="store_true", default=False)
    parser.add_argument(
        "--write_only", "-w", help="Only write (generate) files, don't read",
        action="store_true", default=False)
    parser.add_argument(
        "peer_ips", help="comma delimited list of host IPs to use for "
        "benchmarking")

    args = parser.parse_args()
    binary = os.path.join(BENCHMARK_DIR, "storagebench")
    # Run the storage benchmark individually on each machine as if it were its
    # own cluster of size 1.
    solo_mode = True

    if args.read_only and args.write_only:
        sys.exit("Cannot specify both read-only and write-only")

    if args.write_only:
        read = 0
    else:
        read = 1

    if args.read_only:
        write = 0
    else:
        write = 1

    if read == 1 and write == 1:
        stage_stats = "reader,writer"
    elif read == 1 and write == 0:
        stage_stats = "reader"
    elif read == 0 and write == 1:
        stage_stats = "writer"
    else:
        sys.exit("Cannot specify both read-only and write-only")

    # Pass read/write params to Themis
    params = "-READ %d -WRITE %d" % (read, write)
    print params

    run_benchmark_iterations(
        binary, args.log_directory, args.config, args.peer_ips, args.profiler,
        args.profiler_options, args.iterations, args.sleep, args.delete_output,
        args.per_peer_config, args.dump_core_directory, solo_mode, stage_stats,
        None, params)

if __name__ == "__main__":
    sys.exit(main())
