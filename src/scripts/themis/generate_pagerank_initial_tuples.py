#!/usr/bin/env python

import os, sys, argparse, struct

sys.path.append(os.path.join(os.path.abspath(
            os.path.dirname(__file__)), os.pardir))

from disks.dfs.node_dfs import dfs_get_local_paths, dfs_mkdir

def generate_pagerank_initial_tuples(directory, num_input_disks):
    # Create directory on DFS (recursively creating sub-directories) if it
    # doesn't already exist
    dfs_mkdir(directory, True)

    # Get the local disks corresponding to that directory
    local_paths = dfs_get_local_paths(directory)[:num_input_disks]

    num_local_paths = len(local_paths)

    for (local_path_id, local_path) in enumerate(local_paths):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        local_file = os.path.join(local_path, "input")

        with open(local_file, 'wb+') as fp:
            packed_tuple = struct.pack("IIQQ", 8, 8, local_path_id,
                                       num_local_paths)
            fp.write(packed_tuple)

def main():
    parser = argparse.ArgumentParser(description="generate an input file per "
                                     "disk in the provided DFS directory "
                                     "containing a tuple whose key is 0..n "
                                     "and whose value is n, where n is the "
                                     "number of input directories on this node")
    parser.add_argument("--num_input_disks", "-n", help="number of disks to "
                        "use as input disks", type=int, default=8)
    parser.add_argument("directory", help="the directory on distributed "
                        "storage in which to create the input files. Will be "
                        "created if it doesn't already exist")

    args = parser.parse_args()

    generate_pagerank_initial_tuples(**vars(args))

if __name__ == "__main__":
    main()
