#!/usr/bin/env python

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time

vargensort_bin = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, os.pardir,
    "vargensort", "vargensort")
valsort_bin = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, os.pardir,
    "gensort", "valsort_mr")

# Only match lower case hex checksums
checksum_regex = re.compile("^Checksum: ([0-9a-f]+)$")
sorted_regex = re.compile("^SUCCESS -")


def human_readable_time(time_in_seconds):
    """
    Given a time in seconds, return a timestamp-like string of the form HH:MM:SS
    """
    hours = int(time_in_seconds / 3600)
    time_in_seconds -= hours * 3600
    minutes = int(time_in_seconds / 60)
    time_in_seconds -= minutes * 60
    time_in_seconds = int(time_in_seconds)
    return "%02d:%02d:%02d" % (hours, minutes, time_in_seconds)

def runAllExperiments(timeout, config_file, disk, sort_strategies,
                      max_key_lengths, max_value_lengths, pareto_as, pareto_bs,
                      byte_counts, validate=False):
    """
    Sweep the parameter space for all possible experiments and run them all one
    by one. In addition to the benchmark's input and output files, this function
    also creates stats json files and an arguments json file. The stats files
    record individual experiment parameters, as well as results.
    """
    config_file = os.path.abspath(config_file)
    disk = os.path.abspath(disk)
    # Interpret arguments as comma delimited lists
    sort_strategies = sort_strategies.split(",")
    max_key_lengths = max_key_lengths.split(",")
    max_value_lengths = max_value_lengths.split(",")
    pareto_as = pareto_as.split(",")
    pareto_bs = pareto_bs.split(",")
    byte_counts = byte_counts.split(",")

    # Sweep the 6 dimensional space of experiments
    experiments = [(sort_strategy, max_key_length, max_value_length, pareto_a,
                    pareto_b, byte_count)
                   for sort_strategy in sort_strategies
                   for max_key_length in max_key_lengths
                   for max_value_length in max_value_lengths
                   for pareto_a in pareto_as
                   for pareto_b in pareto_bs
                   for byte_count in byte_counts]
    num_files = len(experiments)

    sorted_files = []
    unsorted_files = []
    failed_files = []
    timeout_files = []

    file_id = 0
    start_time = time.time()
    for (sort_strategy, max_key_length, max_value_length, pareto_a, pareto_b,
         byte_count) in experiments:
        # Pretty print the progress, including timestamp, elapsed time, and ETA
        now = time.time()
        elapsed = now - start_time
        eta = "NA"
        if file_id > 0:
            eta = human_readable_time(
                (elapsed / file_id) * (num_files - file_id))
        elapsed = human_readable_time(elapsed)
        now = time.strftime("%H:%M:%S")
        print "[%s] %d / %d (%.2f%%) Elapsed [%s] ETA [%s]" % (
            now, file_id, num_files, (float(file_id) / num_files) * 100.0,
            elapsed, eta)

        # Create command line string to run an experiment
        command = ("./sortstrategybench -CONFIG %s -DISK %s -VARGENSORT_BIN %s "
                   "-SORT_STRATEGY %s -MAX_KEY_LENGTH %s -MAX_VALUE_LENGTH %s "
                   "-PARETO_A %s -PARETO_B %s -BYTES %s -FILE_ID %s") % (
            config_file, disk, vargensort_bin, sort_strategy, max_key_length,
            max_value_length, pareto_a, pareto_b, byte_count, file_id)
        command = shlex.split(command)

        # Start the experiment asynchronously
        command_object = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Poll for termination once per second until a timeout has elapsed
        remaining_attempts = timeout
        while remaining_attempts > 0 and command_object.returncode == None:
            # Wait 1 second
            time.sleep(1)
            remaining_attempts -= 1
            # Check for termination
            command_object.poll()

        if command_object.returncode == None:
            # The command timed out, so kill it.
            command_object.kill()
            timeout_files.append(file_id)
        else:
            # The command terminated. Check for successful completion.
            (stdout_data, stderr_data) = command_object.communicate()
            if command_object.returncode != 0:
                sys.exit("Command '%s' failed: \nstdout:\n%s\nstderr:\n%s" % (
                        ' '.join(command), stdout_data, stderr_data))

            if validate:
                # Run valsort_mr on both input and output files
                command = ("%s %s/input_%s" % (valsort_bin, disk, file_id))
                command = shlex.split(command)
                command_object = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (input_stdout, input_stderr) = command_object.communicate()

                input_stdout = input_stdout.split("\n")
                input_stderr = input_stderr.split("\n")
                input_data = input_stdout + input_stderr

                command = ("%s %s/output_%s_0" % (valsort_bin, disk, file_id))
                command = shlex.split(command)
                command_object = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (output_stdout, output_stderr) = command_object.communicate()

                output_stdout = output_stdout.split("\n")
                output_stderr = output_stderr.split("\n")
                output_data = output_stdout + output_stderr

                # Search for input checksum
                input_checksum = None
                for line in input_data:
                    checksum_match = checksum_regex.match(line)
                    if checksum_match is not None:
                        input_checksum = checksum_match.group(1)
                        break

                # Search for output checksum and sortedness
                output_checksum = None
                output_sorted = False
                for line in output_data:
                    checksum_match = checksum_regex.match(line)
                    if checksum_match is not None:
                        output_checksum = checksum_match.group(1)
                    sorted_match = sorted_regex.match(line)
                    if sorted_match is not None:
                        output_sorted = True

                # If the output checksum wasn't found, then the run was aborted
                # because the scratch space was too large.
                if output_checksum is None:
                    failed_files.append(file_id)
                else:
                    # The run completed. Record sortedness and checksums
                    if not output_sorted:
                        unsorted_files.append(file_id)
                    else:
                        sorted_files.append(file_id)

                    json_file_path = os.path.join(
                        disk, "stats_%d.json" % file_id)
                    json_file = open(json_file_path, "r")
                    json_object = json.load(json_file)

                    json_object["input_checksum"] = input_checksum
                    json_object["output_checksum"] = output_checksum
                    json_object["output_sorted"] = output_sorted

                    json_file.close()
                    json_file = open(json_file_path, "w")
                    json.dump(json_object, json_file, indent=2)

                    json_file.close()

        file_id += 1

    # Dump script arguments to a file
    arguments_file_path = os.path.join(disk, "script_arguments.json")
    arguments_file = open(arguments_file_path, "w")

    arguments_json = {}
    arguments_json["config_file"] = config_file
    arguments_json["disk"] = disk
    arguments_json["sort_strategies"] = sort_strategies
    arguments_json["max_key_lengths"] = max_key_lengths
    arguments_json["max_value_lengths"] = max_value_lengths
    arguments_json["pareto_as"] = pareto_as
    arguments_json["pareto_bs"] = pareto_bs
    arguments_json["byte_counts"] = byte_counts
    arguments_json["timeout"] = timeout

    json.dump(arguments_json, arguments_file, indent=2)
    arguments_file.close()

    #  Print correctness information
    print "Failed experiments: %s" % (failed_files)
    print "Timed out experiments: %s" % (timeout_files)
    if validate:
        print "Unsorted files: %s" % (unsorted_files)
        print "Number of successfully sorted files: %d / %d" % (
            len(sorted_files), num_files)
        if len(unsorted_files) > 0:
            print "FAILURE"
        else:
            print "SUCCESS"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="drives the experiments for the sort strategy benchmark")

    parser.add_argument(
        "timeout", type=int, help="timeout in seconds for a long-running sort")

    parser.add_argument(
        "config_file", help="config file to pass to benchmark executable")
    parser.add_argument(
        "disk", help="location where all input, output, and stat files will be"
        "stored")

    parser.add_argument(
        "sort_strategies", help="comma delimited list of sort strategies")
    parser.add_argument(
        "max_key_lengths", help="comma delimited list of max key lengths")
    parser.add_argument(
        "max_value_lengths", help="comma delimited list of max value lengths")
    parser.add_argument(
        "pareto_as", help="comma delimited list of pareto a values")
    parser.add_argument(
        "pareto_bs", help="comma delimited list of pareto b values")
    parser.add_argument(
        "byte_counts", help="comma delimited list of bytes counts")

    parser.add_argument(
        "--validate", "-v", help="validate sort correctness using valsort_mr",
        default=False, action="store_true")

    args = parser.parse_args()

    runAllExperiments(**vars(args))
