#!/usr/bin/env python

import os, sys, argparse, subprocess

SCRIPT_DIR = os.path.abspath(
    os.path.dirname(__file__))

sys.path.append(os.path.join(SCRIPT_DIR, os.pardir))

from disks.dfs.node_dfs import dfs_get_local_paths, dfs_mkdir

def generate_synthetic_click_logs(
    directory, num_input_disks, data_size, max_clicks):

    # Create the directory on DFS if it doesn't exist
    dfs_mkdir(directory, True)

    # Get local paths corresponding to DFS directory for the first
    # num_input_disks disks
    local_paths = dfs_get_local_paths(directory)[:num_input_disks]

    local_files = []

    # Make a path for a local file on each disk storing click logs, dividing
    # the number of bytes to generate over those files evenly

    for (local_path_id, local_path) in enumerate(local_paths):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        local_file = os.path.join(local_path, "click_logs")
        local_files.append(
            [local_file, data_size / num_input_disks])

    # Add the extra few bytes arbitrarily to the first local file
    local_files[0][1] += data_size % num_input_disks

    generator_path = os.path.abspath(
        os.path.join(
            SCRIPT_DIR, os.pardir, os.pardir, "gen_synthetic_click_logs",
            "gen_synthetic_click_logs"))

    if not os.path.exists(generator_path):
        sys.exit("Can't find '%s'. It's possible you didn't build it, or "
                 "haven't symlinked it if you're doing an out-of-source build"
                 % (generator_path))

    cmd_template = "%s %%(output_file)s %d %%(num_bytes)s" % (
        generator_path, max_clicks)

    pending_cmds = []

    for (filename, num_bytes) in local_files:
        command = cmd_template % {
            "output_file" : filename,
            "num_bytes" : num_bytes
            }

        pending_cmds.append(subprocess.Popen(command, shell=True))

    returncode = 0

    for pending_cmd in pending_cmds:
        pending_cmd.communicate()

        returncode = max(pending_cmd.returncode, returncode)

    return returncode

def main():
    parser = argparse.ArgumentParser(
        description="generate synthetic click logs on this host's disks")

    parser.add_argument(
        "--num_input_disks", "-n", help="number of disks to use as input disks",
        default=8, type=int)
    parser.add_argument(
        "directory", help="directory relative to the root of each disk on "
        "which click log files will be created")
    parser.add_argument(
        "data_size", help="number of bytes of click logs (approx) that should "
        "be created on this host. This number is divided roughly evenly "
        "across disks", type=int)
    parser.add_argument("max_clicks", help="maximum number of clicks in a "
                        "session", type=int)

    args = parser.parse_args()

    generate_synthetic_click_logs(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
