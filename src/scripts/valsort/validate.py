#!/usr/bin/env python

import argparse, os, subprocess, sys, redis, json

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
THEMIS_SCRIPTS_PATH = os.path.join(SCRIPT_DIR_PATH, os.pardir, "themis")

sys.path.append(THEMIS_SCRIPTS_PATH)

import utils

sys.path.append(os.path.join(THEMIS_SCRIPTS_PATH, "job_runner"))

from redis_utils import CoordinatorDB

SHELL_SCRIPTS_PATH = os.path.join(SCRIPT_DIR_PATH, "shell_scripts")

GENERATE_VALIDATION_SCRIPTS = os.path.join(
    SHELL_SCRIPTS_PATH, "generate_validation_scripts.sh")
RUN_VALIDATION_SCRIPTS = os.path.join(
    SHELL_SCRIPTS_PATH, "run_validation_scripts.sh")
CAT_SUMFILES = os.path.join(
    SHELL_SCRIPTS_PATH, "cat_sumfiles.sh")
GATHER_CATFILES = os.path.join(
    SHELL_SCRIPTS_PATH, "gather_catfiles.sh")
GATHER_SUMFILES = os.path.join(
    SHELL_SCRIPTS_PATH, "gather_sumfiles.sh")
CAT_CATFILES = os.path.join(
    SHELL_SCRIPTS_PATH, "cat_catfiles.sh")
FINAL_VALSORT_SUMMARY = os.path.join(
    SHELL_SCRIPTS_PATH, "final_valsort_summary.sh")

VALSORT_2013_BINARY=os.path.join(
    SCRIPT_DIR_PATH, os.pardir, os.pardir, "gensort_2013", "valsort_2013")
OLD_VALSORT_BINARY=os.path.join(
    SCRIPT_DIR_PATH, os.pardir, os.pardir, "gensort", "valsort")
OLD_VALSORT_BINARY_MR=os.path.join(
    SCRIPT_DIR_PATH, os.pardir, os.pardir, "gensort", "valsort_mr")

def print_common_checksums():
    print "Common checksums: "
    print "  Uniform:"
    print "    10 GB - 2faf4162801872c"
    print "    20 GB - 5f5f60b23024285"
    print "    40 GB - bebd46a2cb44934"
    print "    80 GB - 17d79a990e0ff57e"
    print "    100 GB - 1dcd7f0bb4142463"
    print "    200 GB - 3b9b017f6baaffac"
    print "    600 GB - b2d0481c6b39a938"
    print "    1200 GB - 165a06309dd23e785"
    print "    2400 GB - 2cb40fc1cb95a9059"
    print "    100000 GB - 746a51007040ea07ed"
    print "  Skewed:"
    print "    80 GB - 17d7a18832550f50"
    print "    100 GB - 1dcd8124292c1127"
    print "    200 GB - 3b9b1e2d1e27caee"
    print "    100000 GB - 746a50ec9293190d87"
    print ""

def parallel_ssh(
    hosts, script_path, script_options, verbose, print_stdout=False):
    # Script path is either a string, or a list of strings one per host
    if not isinstance(script_options, str):
        # Must be a list of same length as hosts
        if not isinstance(script_options, list):
            sys.exit(
                "Script options must either be a string or a list of options "
                "one per host. Got %s" % script_options)
        if len(script_options) != len(hosts):
            sys.exit(
                "Script options list must be same length as hosts (%d) but got "
                "%s" % len(hosts), script_options)

    # Launch all ssh commands in parallel.
    print "Running %s in parallel on %d hosts..." % (script_path, len(hosts))

    pending_commands = []
    for host_ID, host in enumerate(hosts):
        command_template = ('%(ssh)s %(host)s "%(script_path)s '
                            '%(script_options)s"')

        options_string = script_options
        if isinstance(options_string, list):
            # Pick the option for this host
            options_string = options_string[host_ID]

        command = command_template % {
            "ssh" : utils.ssh_command(),
            "host" : host,
            "script_path" : script_path,
            "script_options" : options_string,
            }

        command_object = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        pending_commands.append((command_object, command, host))

    # Wait for each one to complete.
    failed_hosts = []
    for (command_object, command_string, host) in pending_commands:
        (stdout, stderr) = command_object.communicate()

        if verbose:
            print "%s:%s" % (host, command_string)
        if command_object.returncode != 0:
            print "  FAILURE - returned %d:" % command_object.returncode
            print "  stdout:"
            if stdout is not None:
                for line in stdout.split('\n'):
                    print "    %s" % line
            print "  stderr:"
            if stderr is not None:
                for line in stderr.split('\n'):
                    print "    %s" % line
            failed_hosts.append(host)
        else:
            if verbose:
                print "  SUCCESS!"
            if print_stdout:
                print "  stdout:"
                if stdout is not None:
                    for line in stdout.split('\n'):
                        print "    %s" % line

    if len(failed_hosts) > 0:
        print ""
        print "Parallel %s failed on hosts:" % script_path
        print "  %s" % failed_hosts
        return False

    return True

def validate(
    job_specification_file, job, parallel, cleanup, old_valsort,
    intermediates, verbose, yes, redis_host, redis_port, redis_db, replica,
    input_on_intermediate_disks):

    if intermediates and not job:
        sys.exit("Must specify job ID for intermediate data validation")


    # If the user just wants to cleanup, don't run validation scripts
    run_validation = not cleanup

    # Connect to redis.
    redis = CoordinatorDB(redis_host, redis_port, redis_db)

    # Pull input and output directory from the job spec file
    with open(job_specification_file, 'r') as fp:
        job_specs = json.load(fp)

    if not isinstance(job_specs, dict):
        sys.exit("Graysort job spec should be a single job. Got a list.")

    input_dir = job_specs["input_directory"]
    intermediate_dir = job_specs["intermediate_directory"]
    output_dir = job_specs["output_directory"]

    # We're only going to validate local data
    input_prefix = input_dir[:9]
    if input_prefix != "local:///":
        sys.exit(
            "Can only validate local data but got input dir %s" % input_dir)
    intermediate_prefix = intermediate_dir[:9]
    if intermediate_prefix != "local:///":
        sys.exit(
            "Can only validate local data but got intermediate dir %s" % \
                intermediate_dir)
    output_prefix = output_dir[:9]
    if output_prefix != "local:///":
        sys.exit(
            "Can only validate local data but got output dir %s" % output_dir)

    # Strip local prefix
    input_dir = input_dir[9:]
    intermediate_dir = intermediate_dir[9:]
    output_dir = output_dir[9:]

    # Determine whether we're validating input, intermediate, or output files
    data_dir = None
    mapreduce = True
    if job == None:
        # Input files
        data_dir = input_dir
        if replica:
            data_dir += "_replica"
        print "Validating input files stored in /%s" % data_dir

        # Check for mapreduce headers on inputs files.
        if "params" in job_specs and \
                "MAP_INPUT_FORMAT_READER" in job_specs["params"] and \
                job_specs["params"]["MAP_INPUT_FORMAT_READER"] == \
                "FixedSizeKVPairFormatReader":
            # Make sure the key and value sizes are right.
            if job_specs["params"]["MAP_INPUT_FIXED_KEY_LENGTH"] != 10 or \
                    job_specs["params"]["MAP_INPUT_FIXED_VALUE_LENGTH"] != 90:
                sys.exit("Can only validate fixed size tuples with 10 byte "
                         "keys and 90 byte values")
            mapreduce = False
    elif intermediates:
        # Intermediate files
        replica_suffix = ""
        if replica:
            # Validate replica files instead
            replica_suffix = "_replica"
        data_dir = os.path.join(intermediate_dir, "job_%d%s" % (job, replica_suffix))
        print "Validating intermediate files stored in /%s" % data_dir

        # Check for mapreduce headers on intermediate files
        if "params" in job_specs and \
                "WRITE_WITHOUT_HEADERS.phase_one" in job_specs["params"] and \
                job_specs["params"]["WRITE_WITHOUT_HEADERS.phase_one"] == 1:
            mapreduce = False
    else:
        # Output files
        replica_suffix = ""
        if replica:
            # Validate replica files instead
            replica_suffix = "_replica"
        data_dir = os.path.join(output_dir, "job_%d%s" % (job, replica_suffix))
        print "Validating output files stored in /%s" % data_dir

        # Check for mapreduce headers on output files
        if "params" in job_specs and \
                "WRITE_WITHOUT_HEADERS.phase_two" in job_specs["params"] and \
                job_specs["params"]["WRITE_WITHOUT_HEADERS.phase_two"] == 1:
            mapreduce = False

    # Pick valsort output directory based on job spec file name
    job_name = os.path.basename(job_specification_file.split(".json")[0])
    if job != None:
        # Append job ID if we're doing output files
        job_name = "%s_job_%d" % (job_name, job)
        # Append intermediate if we're doing intermediates
        if intermediates:
            job_name = "%s_intermediates" % job_name

    valsort_output_dir = os.path.join("/tmp", "valsort", job_name)
    print "Valsort outputs will be stored in %s" % valsort_output_dir

    # Pick valsort binary and options
    valsort_binary = None
    if old_valsort:
        if mapreduce:
            valsort_binary = OLD_VALSORT_BINARY_MR
        else:
            valsort_binary = OLD_VALSORT_BINARY
    else:
        valsort_binary = VALSORT_2013_BINARY
        if mapreduce:
            valsort_binary = "%s -m" % valsort_binary

    # Ask redis for the list of nodes
    host_list = list(redis.known_nodes)
    host_list.sort()

    if run_validation:
        # Run validation scripts in parallel on each node.
        # STEP 1
        # Generate validation shell scripts
        script_options_prefix = "%s '%s' %d" % (
            valsort_output_dir, valsort_binary, parallel)
        # Pull the disk lists for each individual node from redis.
        script_options = []
        for host in host_list:
            # Intermediate data goes on local disks
            if intermediates or input_on_intermediate_disks:
                disk_list = redis.local_disks(host)
            else:
                disk_list = redis.io_disks(host)
            disk_list = [os.path.join(disk, data_dir) for disk in disk_list]
            disk_list = " ".join(disk_list)
            script_options.append("%s %s" % (script_options_prefix, disk_list))

        # Now run the parallel ssh command
        script = GENERATE_VALIDATION_SCRIPTS
        success = parallel_ssh(host_list, script, script_options, verbose)
        if not success:
            sys.exit("%s failed" % script)

        # STEP 2
        # Run the actual validation scripts themselves
        script = RUN_VALIDATION_SCRIPTS
        parallel_ssh(host_list, script, valsort_output_dir, verbose)
        if not success:
            sys.exit("%s failed" % script)

        if replica:
            # STEP 3
            # Gather sumfiles to head node
            head_node = [host_list[0]]
            script = GATHER_SUMFILES
            host_list_string = " ".join(host_list)
            script_options = "%s %s" % (valsort_output_dir, host_list_string)
            parallel_ssh(head_node, script, script_options, verbose)
            if not success:
                sys.exit("%s failed" % script)

            # STEP 4
            # Cat sumfiles on the head node
            script = CAT_SUMFILES
            head_node = [host_list[0]]
            parallel_ssh(head_node, script, valsort_output_dir, verbose)
            if not success:
                sys.exit("%s failed" % script)
        else:
            # STEP 3
            # Cat sumfiles locally
            script = CAT_SUMFILES
            parallel_ssh(host_list, script, valsort_output_dir, verbose)
            if not success:
                sys.exit("%s failed" % script)

            # STEP 4
            # Gather catfiles to the head node
            head_node = [host_list[0]]
            script = GATHER_CATFILES
            host_list_string = " ".join(host_list)
            script_options = "%s %s" % (valsort_output_dir, host_list_string)
            parallel_ssh(head_node, script, script_options, verbose)
            if not success:
                sys.exit("%s failed" % script)

        # STEP 5
        # Cat all catfiles on the head node
        script = CAT_CATFILES
        parallel_ssh(head_node, script, valsort_output_dir, verbose)
        if not success:
            sys.exit("%s failed" % script)

        # STEP 6
        # Run valsort summary on the final catfile and print the results
        script = FINAL_VALSORT_SUMMARY
        script_options = "%s '%s'" % (valsort_output_dir, valsort_binary)
        parallel_ssh(
            head_node, script, script_options, verbose, print_stdout=True)
        if not success:
            sys.exit("%s failed" % script)

    # Ask user to delete files
    if yes:
        delete_files = "yes"
    else:
        delete_files = raw_input(
            "Delete all files? <rm -rf %s> (yes/no)? " % valsort_output_dir)
    if delete_files == "yes":
        # Paranoia check:
        if valsort_output_dir == "/":
            sys.exit("Can't delete /")

        success = parallel_ssh(
            host_list, "rm", "-rf %s" % valsort_output_dir, verbose)
        if not success:
            sys.exit("Valsort output deletion failed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validates graysort input or output files")
    utils.add_redis_params(parser)

    parser.add_argument(
        "job_specification_file", help="a JSON file giving enough information "
        "about the job for Themis to run it")
    parser.add_argument(
        "-j", "--job", help="if specified, validate output files for this job "
        "ID, otherwise validate input files", type=int, default=None)
    parser.add_argument(
        "-p", "--parallel", type=int, default=1,
        help="number of parallel workers to use per disk (default %(default)s)")
    parser.add_argument(
        "-c", "--cleanup", default=False, action="store_true",
        help="Clean up by only deleting valsort outputs instead of validating")
    parser.add_argument(
        "-o", "--old_valsort", default=False, action="store_true",
        help="Use the old valsort binary instead of the 2013 version")
    parser.add_argument(
        "-i", "--intermediates", default=False, action="store_true",
        help="Validate intermediate data for a job instead of output data")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed parallel ssh information")
    parser.add_argument(
        "-y", "--yes", default=False, action="store_true",
        help="Answer yes to all questions (automatically clean up valsort output)")
    parser.add_argument(
        "-r", "--replica", default=False, action="store_true",
        help="Validate replica files instead of output files")
    parser.add_argument(
        "--input_on_intermediate_disks", default=False, action="store_true",
        help="Validate input data on intermediate disks.")


    args = parser.parse_args()

    print_common_checksums()

    validate(**vars(args))
