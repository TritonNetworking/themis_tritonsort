#!/usr/bin/env python

import os, sys, subprocess, getpass, argparse, socket, redis, random, shlex
from plumbum.cmd import chmod, chown, mkdir, sudo

BASE_SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(
            os.path.abspath(__file__)), os.pardir))

sys.path.append(BASE_SCRIPTS_DIR)
from common import unitconversion
from themis import utils

# Generate assignment of data to disks in the cluster.
# Takes as input a mapping of host -> disk list and the total data size
# Returns a mapping of host -> disk list tuples containing (offset, data_size)
def generate_data_assignment(io_disk_map, data_size, multiple):
    num_disks = len(sum(io_disk_map.values(), []))
    num_nodes = len(io_disk_map)

    # Divide data evenly across disks in the cluster.
    data_per_disk = int(data_size) / num_disks
    # Make sure we assign in multiples of the specified value
    data_per_disk = (data_per_disk / multiple) * multiple
    remainder = int(data_size) - (data_per_disk * num_disks)

    if data_per_disk == 0:
        # Special case for small data. Round robin data across nodes rather
        # than across disks
        data_per_node = data_size / num_nodes
        remainder = data_size % num_nodes

    # Compute offset, data size pairs
    pairs = []
    offset = 0
    for host, disks in io_disk_map.iteritems():
        if data_per_disk == 0:
            # Small data: Round robin data across nodes.
            disks_to_assign = data_per_node
            if remainder > 0:
                disks_to_assign += 1
                remainder -= 1
            for i, disk in enumerate(disks):
                if i < disks_to_assign:
                    pairs.append((offset, 1))
                    offset += 1
                else:
                    pairs.append((offset, 0))
        else:
            # Common case: Round robin data across disks.
            for disk in disks:
                data_to_assign = data_per_disk
                if remainder > 0:
                    assigned_remainder = min(remainder, multiple)
                    data_to_assign += assigned_remainder
                    remainder -= assigned_remainder
                pairs.append((offset, data_to_assign))
                offset += data_to_assign

    assignments = {}
    round_number = 0
    index = 0
    while index < len(pairs):
        for host in io_disk_map:
            if len(io_disk_map[host]) > round_number:
                if round_number == 0:
                    assignments[host] = []
                assignments[host].append(pairs[index])
                index += 1

        round_number += 1

    return assignments

def generate_graysort_inputs(
    redis_host, redis_port, redis_db, total_data_size, method, debug, pareto_a,
    pareto_b, max_key_len, max_val_len, min_key_len, min_val_len, large_tuples,
    gensort_command,
    username, no_sudo, skew, graysort_compatibility_mode,
    num_files_per_disk, multiple, replica_number, parallelism,
    intermediate_disks):

    disk_list_key = "node_io_disks"
    if intermediate_disks:
        # Write to intermediate disks instead.
        disk_list_key = "node_local_disks"

    # Connect to Redis.
    redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db)

    # Get list of hosts from redis.
    hosts = list(redis_client.smembers("nodes"))
    hosts.sort()

    # Generate a mapping of hosts -> io disks
    io_disk_map = {}
    first_partition_map = {}
    num_partitions = 0
    for host in hosts:
        disks = list(redis_client.smembers("%s:%s" % (disk_list_key, host)))
        disks.sort()
        disk_list = []
        for disk in disks:
            for f in xrange(num_files_per_disk):
                disk_list.append(disk)
        io_disk_map[host] = disk_list
        first_partition_map[host] = num_partitions
        num_partitions += len(disk_list)

    local_fqdn = socket.getfqdn()
    if replica_number > 1:
        hosts = io_disk_map.keys()
        host_index = hosts.index(local_fqdn)
        # Act as if we are a different host for the purposes of generating
        # replica files
        host_index = (host_index + (replica_number - 1)) % len(io_disk_map)
        local_fqdn = hosts[host_index]


    # Get list of input disks for this local machine.
    input_disks = io_disk_map[local_fqdn]
    first_partition = first_partition_map[local_fqdn]

    job_name = "Graysort"
    if not graysort_compatibility_mode:
        job_name = "Graysort-MapReduceHeaders"
    if replica_number > 1:
        job_name += "_replica"
    input_directory = os.path.join(username, "inputs", job_name)

    # Compute input file names.
    input_file_relative_paths = map(
        lambda x: os.path.join(
            input_directory, "%08d.partition" % (first_partition + x)),
        xrange(len(input_disks)))
    input_files = [os.path.abspath(os.path.join(disk, relative_path))
                   for disk, relative_path
                   in zip(input_disks, input_file_relative_paths)]

    # Find out if data already exists.
    existing_files = filter(os.path.exists, input_files)
    if len(existing_files) == len(input_files):
        print >>sys.stderr, "Data already exists"
        sys.exit(0)

    # Data needs to be generated. Delete any existing files.
    for input_file in existing_files:
        os.remove(input_file)

    # Convert human-readable data size to records or bytes.
    unit = "R"
    if method == "pareto":
        # Pareto uses bytes since records are variably sized.
        unit = "B"
    data_size = unitconversion.parse_and_convert(total_data_size, unit)

    # Compute record assignments to disks in the cluster.
    assignments = generate_data_assignment(io_disk_map, data_size, multiple)
    local_assignments = assignments[local_fqdn]

    # If large tuples were specified, assign them round robin across disks.
    if large_tuples is not None:
        large_tuple_assignments = {}
        # Convert comma-delimited list into a list of triples.
        large_tuples = large_tuples.split(",")
        large_tuples = zip(
            large_tuples[0::3], large_tuples[1::3], large_tuples[2::3])
        # Round robin the triples across disks.
        for index, large_tuple in enumerate(large_tuples):
            disk_index = index % len(input_disks)
            if disk_index not in large_tuple_assignments:
                large_tuple_assignments[disk_index] = list(large_tuple)
            else:
                large_tuple_assignments[disk_index].extend(list(large_tuple))

    # Finally we're ready to create input files on each disk.
    gensort_commands = []
    for disk_index, input_file in enumerate(input_files):
        # Manually create directories.
        # Create input directory
        directory = os.path.abspath(os.path.join(input_file, os.pardir))
        if no_sudo:
            cmd = mkdir["-p", directory]
        else:
            cmd = sudo[mkdir["-p", directory]]

        if debug:
            print cmd
        else:
            cmd()

        # Change ownership of input directory from root to USER
        if not no_sudo:
            cmd = sudo[chown["-R", username, os.path.dirname(directory)]]

            if debug:
                print cmd
            else:
                cmd()

        # PREPARE gensort command.
        (disk_data_offset, disk_data_size) = local_assignments[disk_index]
        command_options = {}
        command_args = []

        if method == "pareto":
            # Set all pareto distribution options in an options string.
            command_options["-pareto_a"] = "%f" % (pareto_a)
            command_options["-pareto_b"] = "%f" % (pareto_b)
            command_options["-maxKeyLen"] = "%d" % (max_key_len)
            command_options["-maxValLen"] = "%d" % (max_val_len)
            command_options["-minKeyLen"] = "%d" % (min_key_len)
            command_options["-minValLen"] = "%d" % (min_val_len)


            # If large tuples were specified, add them to the options string.
            if (large_tuples is not None and
                disk_index in large_tuple_assignments and
                len(large_tuple_assignments[disk_index]) > 0):

                command_options["-largeTuples"] = ','.join(
                    map(str, large_tuple_assignments[disk_index]))

            command_args = [input_file, "pareto", str(int(disk_data_size))]

        else:
            destination_filename = input_file

            # Set skew and MapReduce mode options
            if skew:
                command_args.append("-s")

            if not graysort_compatibility_mode:
                command_args.append("-m")

            command_args.extend([
                    "-b%d" % (disk_data_offset), str(disk_data_size),
                    destination_filename])

        options_str = ' '.join(('%s %s' % (k, v)
                                for k, v in command_options.items()))

        args_str = ' '.join(command_args)

        command = "%s %s %s" % (gensort_command, options_str, args_str)

        # Finally start the gensort process for this disk.
        if debug:
            print command
        else:
            command = shlex.split(command)
            gensort_commands.append(command)

    # Generate files in random order to attempt to more evenly utilize disks.
    random.shuffle(gensort_commands)
    status = 0
    while len(gensort_commands) > 0:
        running_commands = []
        gensort_processes = []
        for command in gensort_commands:
            print "Running '%s'" % (command)
            gensort_processes.append(subprocess.Popen(
                    command, stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE))

            running_commands.append(command)
            if parallelism > 0 and len(running_commands) >= parallelism:
                # Don't launch more parallel gensort processes
                break

        for command in running_commands:
            gensort_commands.remove(command)

        # Wait for all gensort processes to finish.
        for process in gensort_processes:
            (stdout_data, stderr_data) = process.communicate()
            print "gensort instance completed with return code %d" % (
                process.returncode)

            if process.returncode != 0:
                print stderr_data
                status = 1

    return status

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a node's input files for sort workloads")
    utils.add_redis_params(parser)

    parser.add_argument("total_data_size", help="total data to produce")
    parser.add_argument(
        "method", help="Method used to generate tuples",
        choices=["gensort", "pareto"])
    parser.add_argument(
        "--debug", "-d", help="enable debug mode", default=False,
        action="store_true")
    parser.add_argument(
        "-u", "--username", help="username for whom to generate sort input "
        "(default: %(default)s)", default=getpass.getuser())
    # vargensort arguments
    parser.add_argument(
        "--pareto_a", help="'a'/'alpha' parameter (for Pareto)", type=float)
    parser.add_argument(
        "--pareto_b", help="'b'/'xmin' parameter (for Pareto)", type=float)
    parser.add_argument(
        "--max_key_len", help="Maximum key length allowed (default "
        "%(default)s)", type=int, default=4294967295)
    parser.add_argument(
        "--max_val_len", help="Maximum value length allowed (default "
        "%(default)s)", type=int, default=4294967295)
    parser.add_argument(
        "--min_key_len", help="Minimum key length allowed (default "
        "%(default)s)", type=int, default=10)
    parser.add_argument(
        "--min_val_len", help="Minimum value length allowed (default "
        "%(default)s)", type=int, default=90)
    parser.add_argument(
        "--large_tuples", "-l", help="Location,KeyLen,ValueLen triples of "
        "large tuples to inject with vargensort")
    parser.add_argument(
        "--no_sudo", help="Don't create directories using sudo",
        action="store_true")
    parser.add_argument(
        "--skew", "-s" ,help="Use the skewed distribution for gensort",
        action="store_true")
    parser.add_argument(
        "--graysort_compatibility_mode", "-g", help="If using gensort, "
        "generate tuples without MapReduce headers", action="store_true")
    parser.add_argument(
        "--num_files_per_disk", "-n", help="number of files to create on each "
        "disk", default=1, type=int)
    parser.add_argument(
        "--multiple", "-m", help="Attempt to assign file sizes in multiple of "
        "this value", default=100, type=int)
    parser.add_argument(
        "--replica_number", "-r", help="This desiginates which input file "
        "we are generating. Set to greater than 1 for replication.",
        default=1, type=int)
    parser.add_argument(
        "--parallelism", "-p", help="Use at most this many parallel gensort "
        "invocations at a time", default=0, type=int)
    parser.add_argument(
        "--intermediate_disks", "-i" ,help="Generate files on intermediate "
        "disks instead of input/output disks", default=False,
        action="store_true")

    args = parser.parse_args()

    # Check for method specific required arguments.
    if args.method == "pareto":
        if not args.pareto_a or not args.pareto_b:
            print >>sys.stderr, "Must set pareto arguments a and b"
            sys.exit(1)

    # Build gensort command.
    generators = {
        "gensort": ("gensort", "gensort"),
        "pareto": ("vargensort", "vargensort"),
        }
    (directory, binary) = generators[args.method]
    gensort_command = os.path.abspath(os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, directory, binary))

    if not os.path.exists(gensort_command):
        print >>sys.stderr, "Cannot find '%s'" % gensort_command
        sys.exit(1)

    args.gensort_command = gensort_command

    sys.exit(generate_graysort_inputs(**vars(args)))
