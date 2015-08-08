#!/usr/bin/env python

import os, sys, argparse, json

def path_visitor(file_list, dirname, names):
    for name in names:
        name_abspath = os.path.abspath(os.path.join(dirname, name))
        if os.path.isfile(name_abspath):
            file_length = os.path.getsize(name_abspath)
            file_list.append((name_abspath, file_length))

def list_local_files(directories):
    file_list = []

    for directory in directories:
        if not os.path.exists(directory):
            file_list = None
            break

        files_for_directory = []
        os.path.walk(directory, path_visitor, files_for_directory)
        file_list.append(files_for_directory)

    print json.dumps(file_list)

def main():
    parser = argparse.ArgumentParser(
        description="gets absolute paths and lengths of files in each "
        "directory in the provided directory list as a JSON list")
    parser.add_argument(
        "directories", metavar="dir", nargs='+', help="a directory whose "
        "files should be listed")
    args = parser.parse_args()
    return list_local_files(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
