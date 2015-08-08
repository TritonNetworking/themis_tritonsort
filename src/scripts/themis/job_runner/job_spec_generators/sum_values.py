#!/usr/bin/env python

import sys, argparse, json, utils

def sum_values(input_directory, output_directory, hdfs, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_sumcounts")

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, hdfs)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "ZeroKeyMapFunction",
        reduce_function = "SumValuesReduceFunction")

    utils.force_single_partition(config)

    return config

def main():
    parser = argparse.ArgumentParser(
        description="sum all the values in an input dataset (whose values are "
        "assumed to be numeric)")
    parser.add_argument("input_directory", help="the directory containing the "
                        "files whose values will be summed")
    parser.add_argument("--output_directory", help="the directory that will "
                        "contain the sum as a single record with key '0' "
                        "(default: a sibling directory to input_directory with "
                        "the name '<input directory's name>_sumcounts'")
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="sum_values.json")


    args = parser.parse_args()
    config = sum_values(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
