#!/usr/bin/env python

import sys, argparse, getpass, utils, json

def wordcount(
    username, hdfs, input_directory, output_directory, use_combiner, **kwargs):

    if output_directory is None:
        output_directory = "%s/outputs/wordcount" % (username)

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, hdfs)

    if use_combiner:
        map_function = "CombiningWordCountMapFunction"
    else:
        map_function = "WordCountMapFunction"

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = map_function,
        reduce_function = "WordCountReduceFunction")

    return config

def main():
    parser = argparse.ArgumentParser(
        description="generate a job spec file for the WordCount application")
    parser.add_argument("input_directory", help="the directory containing the "
                        "documents whose words should be counted")
    parser.add_argument("--output_directory", help="the directory that will "
                        "contain the word counts (default: "
                        "<username>/outputs/wordcount)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="wordcount.json")
    parser.add_argument("-u", "--username", help="the username on whose data "
                        "the job is being run", default=getpass.getuser())
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")
    parser.add_argument(
        "--use_combiner", "-c", help="use combiner when generating map "
        "function output", default=False, action="store_true")

    args = parser.parse_args()

    config = wordcount(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
