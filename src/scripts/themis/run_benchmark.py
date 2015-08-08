#!/usr/bin/env python

import os, sys, argparse, shutil, getpass, subprocess, yaml, shlex, json, \
    time, numpy

from metaprograms.runtime_info.gather_runtime_info import gather_runtime_info
from utils import ssh_command, add_interfaces_params

THEMIS_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_SCRIPTS_DIR = os.path.join(THEMIS_SCRIPTS_DIR, os.pardir)
CLUSTER_SCRIPTS_DIR = os.path.join(THEMIS_SCRIPTS_DIR, "cluster")
CLOUD_SCRIPTS_DIR = os.path.join(THEMIS_SCRIPTS_DIR, "cloud")

sys.path.append(BASE_SCRIPTS_DIR)
sys.path.append(CLUSTER_SCRIPTS_DIR)
sys.path.append(CLOUD_SCRIPTS_DIR)

from descriptions.descriptionutils import Description
from parallel_ssh import parallel_ssh
from conf_utils import read_conf_file
from upload_logs import upload_logs
from download_logs import download_logs

def run_benchmark_iterations(
    binary, log_directory, config, peer_ips, profiler, profiler_options,
    iterations, sleep, delete_output, per_peer_config, dump_core_directory,
    solo_mode, stage_stats, interfaces, params=""):

    # Get ssh username and themis directory
    username, themis_directory = read_conf_file(
        "cluster.conf", "cluster", ["username", "themis_directory"])
    themis_directory = os.path.expanduser(themis_directory)
    # Get cloud provider if applicable.
    provider = read_conf_file("cluster.conf", "cluster", "provider")

    if interfaces == None:
        vnstat_interface = None
    else:
        interface_list = filter(lambda x: len(x) > 0, interfaces.split(','))
        vnstat_interface = interface_list[0]

    if not os.path.exists(config):
        sys.exit("Config file %s does not exist." % config)

    with open(config, 'r') as fp:
        app_config = yaml.load(fp)

    # If we're using more than 1 network interface per peer, the peer list is
    # going to look like:
    # Peer1_interface1, Peer1_interface2, Peer2_interface1, Peer2_interface2, ..
    # In this case, we only want to launch the benchmark once per peer, so
    # make sure we only look at the first interface for each peer, and let
    # the application itself deal with the other interfaces.
    num_interfaces = 1
    if "NUM_INTERFACES" in app_config:
        num_interfaces = app_config["NUM_INTERFACES"]

    # Remove trailing comma if any from the IP list. This will be the string we
    # pass into the benchmark binary.
    peer_list = peer_ips.rstrip(",")

    # If we're using multiple interfaces, only launch the benchmark once per
    # node.
    node_list = peer_list.split(",")[::num_interfaces]

    # Look for description files in the same directory as the binary.
    binary_dir = os.path.dirname(binary)
    description_directory = os.path.join(binary_dir, "description")

    if not os.path.exists(description_directory):
        sys.exit("Could not find description directory %s" % (
                description_directory))

    # Check for the phase name. For simplicity we're going to require that
    # the benchmark have only 1 phase
    description = Description(description_directory)
    phases = description.getPhaseList()
    if len(phases) != 1:
        sys.exit("Benchmark must have exactly one phase. Got %s" % phases)
    phase_name = phases[0]

    data_size_per_node = int(
        app_config["BENCHMARK_DATA_SIZE_PER_NODE"][phase_name])
    data_size = data_size_per_node * len(node_list)

    total_throughputs = {}
    if stage_stats is not None:
        stage_stats = stage_stats.split(",")
        for stage in stage_stats:
            total_throughputs[stage] = 0.0

    node_benchmark_throughputs = []

    for i in xrange(iterations):
        # Pick a unique batch ID
        batch = 0
        while os.path.exists(
            os.path.join(log_directory, "batch_%d" % batch)):
            batch += 1
        batch_directory = os.path.join(log_directory, "batch_%d" % batch)

        # Create directories
        phase_directory = os.path.join(batch_directory, phase_name)
        parallel_ssh(
            None, "mkdir -p %s" % phase_directory, username, node_list, False,
            True, False)

        # Copy description files and create phase directory.
        shutil.copy(
            os.path.join(description_directory, "stages.json"),
            batch_directory)
        shutil.copy(
            os.path.join(description_directory, "structure.json"),
            batch_directory)

        # Copy config file
        shutil.copyfile(config, os.path.join(batch_directory, "config.yaml"))

        print "\nLogging to %s" % (batch_directory)
        print "Running %s with batch ID %d on %d nodes..." % (
            phase_name, batch, len(node_list))

        (elapsed, elapsed_times, completed_ips) = run_benchmark(
            binary, config, batch_directory, phase_directory, profiler,
            profiler_options, peer_list, node_list, per_peer_config,
            dump_core_directory, solo_mode, vnstat_interface, params)

        # Compute overall throughput
        throughput = (data_size / elapsed) / 1000000
        per_node_throughput = (data_size_per_node / elapsed) / 1000000
        print "Completed in %.2f seconds." % elapsed
        print "  Throughput: %.2f MB/s" % throughput
        print "  Per-server: %.2f MB/s" % per_node_throughput

        # Record individual throughputs
        throughputs = [(data_size_per_node / x) / 1000000 \
                           for x in elapsed_times]
        node_benchmark_throughputs += throughputs

        # Dump these results to a file in the batch directory
        results_file = open(os.path.join(batch_directory, "results"), "w")
        results_file.write(
            "Runtime: %.2f seconds\nThroughput: %.2f MB/s\nPer-server: " \
                "%.2f MB/s\n\n" % (elapsed, throughput, per_node_throughput))
        results_file.write("Node throughputs: %s\n\n" % throughputs)
        for ip, elapsed_time, throughput in zip(
            completed_ips, elapsed_times, throughputs):
            results_file.write(
                "Node %s completed in %.2f seconds (%.2f MB/s)\n" % (
                    ip, elapsed_time, throughput))
        results_file.write("\n")

        if stage_stats is not None:
            # Compute runtime stat throughputs

            done = False
            while not done:
                # Upload all logs.
                upload_logs()

                # Download logs locally.
                download_logs()

                try:
                    runtime_info = gather_runtime_info(batch_directory, False)
                    done = True
                except ValueError:
                    print "Runtime info script failed. Retrying log upload/downloads."

            stage_info = runtime_info[0]["stages"]
            node_throughputs = {}
            for worker_info in stage_info:
                stats_info = worker_info["stats_info"]
                # We only want to look at the overall stats, which includes all
                # nodes (hostname or worker ID won't be specified)
                if len(stats_info) == 1:
                    stage_name = stats_info["stage"]

                    if stage_name in stage_stats:
                        # This is one of the stages we care about
                        node_throughputs[stage_name] = \
                            worker_info["observed_processing_rate_per_node"]
                        total_throughputs[stage_name] += \
                            node_throughputs[stage_name]

            # Print throughputs in the correct order.
            for stage_name in stage_stats:
                print "  %s throughput: %.2f MB/s/node" % (
                    stage_name, node_throughputs[stage_name])
                results_file.write("%s throughput: %.2f MB/s\n" % (
                        stage_name, node_throughputs[stage_name]))

        results_file.close()

        if delete_output and "OUTPUT_DISK_LIST" in app_config and \
                phase_name in app_config["OUTPUT_DISK_LIST"]:
            output_disk_list = app_config["OUTPUT_DISK_LIST"][phase_name]
            output_disks = output_disk_list.split(",")
            for disk in output_disks:
                print "Clearing %s" % disk
                parallel_ssh(
                    None, "rm -rf %s" % disk, username, node_list, False,
                    False, False)

        if sleep > 0 and i != iterations - 1:
            print "Sleeping %d seconds" % sleep
            time.sleep(sleep)

    print "\nCompleted %d iterations\n" % iterations
    # Format node throughputs
    node_benchmark_throughput_strings = [
        "%.2f" % x for x in node_benchmark_throughputs]
    print "  Node throughputs (MB/s):"
    print "    %s" % node_benchmark_throughput_strings
    print "  Average node throughput: %.2f MB/s" % (
        numpy.mean(node_benchmark_throughputs))
    print "  Standard deviation: %.2f MB/s" % (
        numpy.std(node_benchmark_throughputs))
    print "  Min node throughput: %.2f MB/s" % (
        numpy.min(node_benchmark_throughputs))
    print "  Max node throughput: %.2f MB/s\n" % (
        numpy.max(node_benchmark_throughputs))

    if stage_stats is not None:
        for stage_name in stage_stats:
            print "  Average %s throughput: %.2f MB/s/node" % (
                stage_name, total_throughputs[stage_name] / iterations)

def run_benchmark(
    binary, config, batch_directory, phase_directory, profiler,
    profiler_options, peer_list, node_list, per_peer_config,
    dump_core_directory, solo_mode, vnstat_interface, params):

    # Get ssh username.
    username = read_conf_file("cluster.conf", "cluster", "username")

    # Add command line parameters to binary
    binary = "%s -LOG_DIR %s" % (
        binary, phase_directory)

    if dump_core_directory is not None:
        binary = "cd %s; ulimit -c unlimited; %s" % (
            dump_core_directory, binary)

    processes = []
    start_time = time.time()
    for index, ip in enumerate(node_list):
        # Now start themis binaries
        if solo_mode:
            # Act as if you are the only peer in the cluster.
            peer_binary = "%s -PEER_LIST %s" % (binary, ip)
        else:
            # Use the entire set of peers and designate yourself as
            # one of them.
            peer_binary = "%s -PEER_LIST %s -MYPEERID %d" % (
                binary, peer_list, index)

        if per_peer_config:
            # Append the IP address to the config file name
            peer_binary = "%s -CONFIG %s.%s" % (peer_binary, config, ip)
        else:
            peer_binary = "%s -CONFIG %s" % (peer_binary, config)

        # Override config file with specified parameters
        if params:
            peer_binary = "%s %s" % (peer_binary, params)

        if profiler == "operf":
            # Use the batch directory as the operf session dir
            session_dir = os.path.join(batch_directory, "oprofile_data.%s", ip)
            parallel_ssh(
                None, "mkdir -p %s" % session_dir, username, node_list,
                False, True, False)
            peer_binary = "%s %s --session-dir=%s %s" % (
                profiler, profiler_options, session_dir, peer_binary)
        elif profiler is not None:
            # Some other profiler, just prepend it to the binary
            peer_binary = "%s %s %s" % (
                profiler, profiler_options, peer_binary)

        # Run the node-local benchmark script.
        vnstat_param_string = ""
        if vnstat_interface != None:
            vnstat_param_string = "--vnstat_interface %s" % vnstat_interface
        command = '%s %s "%s/run_benchmark_local.py %s %s \'%s\'"' % (
            ssh_command(), ip, THEMIS_SCRIPTS_DIR, vnstat_param_string,
            phase_directory, peer_binary)

        processes.append((subprocess.Popen(command, shell=True), ip))

    print "%d tasks launched on %s\n" % (len(processes), time.asctime())

    elapsed_times = []
    completed_ips = []

    num_nodes = len(processes)

    while len(processes) > 0:
        for process, ip in processes:
            process.poll()
            if process.returncode != None:
                process.communicate()
                processes.remove((process, ip))
                elapsed_time = time.time() - start_time
                elapsed_times.append(elapsed_time)
                completed_ips.append(ip)
                print "Node %s completed in %.2f seconds (%d / %d)" % (
                    ip, elapsed_time, len(elapsed_times), num_nodes)

                break

    stop_time = time.time()

    return (stop_time - start_time, elapsed_times, completed_ips)

def main():
    parser = argparse.ArgumentParser(
        description="Run a benchmark application on a collection of nodes.")
    parser.add_argument(
        "binary", help="benchmark application binary")
    parser.add_argument(
        "log_directory", help="directory containing logs for an experiment")
    parser.add_argument(
        "config", help="config file to use for the benchmark.")
    parser.add_argument(
        "peer_ips", help="comma delimited list of host IPs to use for "
        "benchmarking")
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
        "--solo_mode", help="run the benchmark on all peers, but run each peer "
        "as if it's its own cluster of size 1.",
        action="store_true", default=False)
    parser.add_argument(
        "--stage_stats", help="comma delimited list of stage names to show "
        "runtime stats for upon completion")
    parser.add_argument(
        "--params", help="params that will override the config file",
        type=str, default="")

    add_interfaces_params(parser)

    args = parser.parse_args()
    run_benchmark_iterations(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
