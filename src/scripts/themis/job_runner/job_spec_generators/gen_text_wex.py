#!/usr/bin/env python

import sys, argparse, utils, json

def gen_text_wex(input_directory, output_directory, hdfs, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_wex_text")

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, hdfs)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "WEXTextExtractorMapFunction",
        reduce_function = "IdentityReduceFunction")

    if "params" not in config:
        config["params"] = {}

    config["params"]["MAP_INPUT_FORMAT_READER"] = "TextLineFormatReader"

    return config

def main():
    parser = argparse.ArgumentParser(
        description="generates lines of text from the WEX dataset")
    parser.add_argument("input_directory", help="the directory containing the "
                        "WEX dataset")
    parser.add_argument(
        "--output_directory", help="the directory that will contain the "
        "text extraction (default: a sibling of the input directory named "
        "'<input directory's name>_wex_text')")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="gen_text_wex.json")
    parser.add_argument("--hdfs", help="host:port specifying the HDFS namenode "
                        "where input and output data should be stored "
                        "(default: store data on local disks rather than on "
                        "HDFS)")


    args = parser.parse_args()
    config = gen_text_wex(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
