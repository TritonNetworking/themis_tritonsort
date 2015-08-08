#!/usr/bin/env python

import sys, argparse, utils, json, os, merge_files

def tuple_length_count(input_directory, output_directory, hdfs, **kwargs):

    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "tuple_length_counter_%(dirname)s")

    intermediate_directory = utils.sibling_directory(
        input_directory, "unmerged_counts_%(dirname)s")

    (input_url, output_url) = utils.generate_urls(
        input_directory, intermediate_directory, hdfs)

    tuple_length_config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "TupleLengthCounterMapFunction",
        reduce_function = "SumValuesReduceFunction")

    merge_files_config = merge_files.merge_files(
        intermediate_directory, output_directory, hdfs)

    config = utils.run_in_sequence(tuple_length_config, merge_files_config)

    return config

def main():
    parser = argparse.ArgumentParser(
        description="count number of times given tuple lengths occur in a "
        "dataset")
    parser.add_argument("input_directory", help="the directory containing the "
                        "documents whose tuples should be analyzed")
    parser.add_argument(
        "--output_directory", help="the directory that will contain the "
        "results of this job (default: a sibling of the input directory named "
        "'tuple_length_counter_<input directory's name>')")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="tuple_length_count.json")
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")

    args = parser.parse_args()
    config = tuple_length_count(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
