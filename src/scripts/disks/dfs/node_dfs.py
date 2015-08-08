#!/usr/bin/env python

import os, sys
from utils import serial_filesystem_operation
import getpass
import logging
from argparse import ArgumentParser

sys.path.append(os.path.abspath(os.path.join(__file__, os.path.pardir,
                                             os.path.pardir)))

from listDisks import listDiskMountpoints

log = logging.getLogger("dfs")

def dfs_get_disks():
    disks = listDiskMountpoints()
    disks.sort()
    return disks

def dfs_mkdir(directory, recursive, start = 0, stop = None):
    disks = dfs_get_disks()

    if stop == None:
        disks = disks[start:]
    else:
        disks = disks[start:stop]

    cmd = "mkdir "

    if recursive:
        cmd += "-p "

    cmd += "${disk}/%s" % (directory)

    serial_filesystem_operation(cmd, disks)

def dfs_get_local_paths(path):
    disks = dfs_get_disks()

    paths = [os.path.abspath(disk + "/" + path) for disk in disks]

    return [p for p in paths if os.path.exists(p)]

def dfs_get_local_paths_cli(args):
    paths = dfs_get_local_paths(args.path)

    if len(paths) > 0:
        print '\n'.join(paths)
    return 0

def dfs_ls(directory, expand_local_paths=False):
    disks = dfs_get_disks()

    cmd = "ls -d1 ${disk}/%s" % (directory)

    if expand_local_paths:
        cmd += "/*"

    outputs = serial_filesystem_operation(cmd, disks, True)

    if outputs == None:
        return None

    flattened_outputs = sum(outputs, [])

    file_set = set()

    for output in flattened_outputs:
        if len(output) > 0:
            file_set.add(output)

    return sorted(file_set)

def dfs_ls_cli(args):
    paths = dfs_ls(directory=args.directory,
                   expand_local_paths=args.expand_local_paths)

    if paths == None:
        print >>sys.stderr, "'ls %s' failed" % (args.directory)
        return 1
    if len(paths) > 0:
        print '\n'.join(paths)
    return 0

def main():
    parser = ArgumentParser(description="command-line interface to several "
                            "node-level DFS commands")
    subparsers = parser.add_subparsers(
        title = "commands", description = "valid subcommands",
        help = "help for node-level DFS commands")

    ls_parser = subparsers.add_parser("ls", help="lists the files in a "
                                      "directory in DFS")
    ls_parser.add_argument("directory", help="directory for which to list "
                           "contents")
    ls_parser.add_argument("--expand_local_paths", "-x",
                           help="expand directory entries to encompass "
                           "local paths", default=False, action="store_true")
    ls_parser.set_defaults(func = dfs_ls_cli)

    mkdir_parser = subparsers.add_parser("mkdir",
                                         help="create a new directory in DFS")
    mkdir_parser.add_argument("directory", help="directory to create")
    mkdir_parser.add_argument("--recursive", "-p", help="create intermediate "
                              "directories along the path if needed")
    mkdir_parser.set_defaults(
        func = lambda args: dfs_mkdir(args.directory, args.recursive))

    get_local_parser = subparsers.add_parser(
        "get_local", help="gets the local path(s) corresponding to a DFS path")
    get_local_parser.add_argument("path", help="the path to query")
    get_local_parser.set_defaults(func = dfs_get_local_paths_cli)

    args = parser.parse_args()

    # Call the appropriate sub-command
    return args.func(args)

if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format='%(levelname)-8s %(asctime)s %(name)-15s %(message)s',
        datefmt='%m-%d %H:%M:%S')

    main()

