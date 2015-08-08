#!/usr/bin/env python

import os, sys, argparse, struct

sys.path.append(os.path.join(os.path.abspath(
            os.path.dirname(__file__)), os.pardir))

from disks.dfs.node_dfs import dfs_get_local_paths, dfs_mkdir

def generate_disk_path_tuples(directory):
    # Create directory on DFS (recursively creating sub-directories) if it
    # doesn't already exist
    dfs_mkdir(directory, True)

    # Get the local disks corresponding to that directory
    local_paths = dfs_get_local_paths(directory)

    for local_path in local_paths:
        benchmark_files_path = os.path.join(local_path, "benchmark_files")

        if not os.path.exists(benchmark_files_path):
            os.makedirs(benchmark_files_path)

        benchmark_files_path_length = len(benchmark_files_path)

        local_file = os.path.join(local_path, "input")

        with open(local_file, 'wb+') as fp:
            packed_tuple = struct.pack("II%ds" % (benchmark_files_path_length),
                                       benchmark_files_path_length, 0,
                                       benchmark_files_path)
            fp.write(packed_tuple)

def main():
    parser = argparse.ArgumentParser(description="generate an input file per "
                                     "disk in the provided DFS directory "
                                     "containing a tuple whose key is the "
                                     "path to the directory on that disk")

    parser.add_argument("directory", help="the directory on distributed "
                        "storage in which to create the input files. Will be "
                        "created if it doesn't already exist")

    args = parser.parse_args()

    generate_disk_path_tuples(**vars(args))

if __name__ == "__main__":
    main()
