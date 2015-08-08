#!/usr/bin/env python

import sys, argparse, utils, json

def merge_files(input_directory, output_directory, hdfs, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_merged")

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, hdfs)

    config = utils.mapreduce_job(
        input_dir = input_url, output_dir = output_url)

    utils.force_single_partition(config)

    return config

def main():
    parser = argparse.ArgumentParser(
        description="merge a collection of input files into a single output "
        "file")
    parser.add_argument("input_directory", help="the directory containing the "
                        "files to be merged")
    parser.add_argument("--output_directory", help="the directory that will "
                        "contain the merged file (default: "
                        "a sibling directory to input_directory with the name "
                        "'<input directory's name>_merged'")
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="merge_files.json")

    args = parser.parse_args()
    config = merge_files(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
