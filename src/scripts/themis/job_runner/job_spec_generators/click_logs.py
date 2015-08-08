#!/usr/bin/env python

import sys, argparse, utils, json

def click_logs(
    input_directory, output_directory, hdfs, session_time_threshold, **kwargs):

    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "session_logs_%(dirname)s")

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, hdfs)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "PassThroughMapFunction",
        reduce_function = "ClickLogSessionSummarizerReduceFunction")

    if "params" not in config:
        config["params"] = {}

    config["params"]["USE_SECONDARY_KEYS"] = 1
    config["params"]["CLICK_LOG_SUMMARIZER_SESSION_TIME_THRESHOLD"] = (
        session_time_threshold)

    return config

def main():
    parser = argparse.ArgumentParser(
        description="run session tracking on click logs satisfying a given "
        "schema")
    parser.add_argument("input_directory", help="the directory containing the "
                        "click logs")
    parser.add_argument("session_time_threshold", help="largest amount of "
                        "time that a session can take", type=int)

    parser.add_argument("--output_directory", help="the directory that will "
                        "contain session information (default: "
                        "a sibling directory to the input directory named "
                        "session_logs_<input directory>)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="click_logs.json")
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")

    args = parser.parse_args()
    config = click_logs(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
