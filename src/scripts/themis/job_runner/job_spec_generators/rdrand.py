#!/usr/bin/env python

import sys, argparse, getpass, utils, json
from merge_files import merge_files

def rdrand(username, input_directory, output_directory, **kwargs):

    if output_directory is None:
        output_directory = "%s/outputs" % (username)
    rdrand_output_directory = "%s/rdrand" % (output_directory)
    merged_output_directory = "%s/final_output" % (output_directory)

    (input_url, rdrand_output_url) = utils.generate_urls(
        input_directory, rdrand_output_directory)
    merged_output_url = utils.generate_url(merged_output_directory)

    rdrand_config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = rdrand_output_url,
        map_function = "PassThroughMapFunction",
        reduce_function = "CountDuplicateKeysReduceFunction",
        partition_function = "UniformPartitionFunction")

    rdrand_params = {
        "SKIP_PHASE_ZERO": 1, # Don't sample...
        "INTERMEDIATE_TO_INPUT_RATIO": 3.0, #... instead assume ratio of 3
        "MAP_INPUT_FORMAT_READER" : "RdRandFormatReader", # 64-bit fragments
        "REDUCE_INPUT_FORMAT_READER": "FixedSizeKVPairFormatReader", # no header
        "REDUCE_INPUT_FIXED_KEY_LENGTH": 16, # 128-bit intermediate keys...
        "REDUCE_INPUT_FIXED_VALUE_LENGTH": 0, # ... with empty values
        "WRITE_WITHOUT_HEADERS.phase_one": 1 # no headers
        }

    if "params" not in rdrand_config:
        rdrand_config["params"] = {}

    for key, value in rdrand_params.items():
        rdrand_config["params"][key] = value

    # Run a second job to merge all duplicate key information into a single
    # output file for better readability.
    mergefiles_config = merge_files(
        rdrand_output_directory, merged_output_directory)

    return utils.run_in_sequence(rdrand_config, mergefiles_config)

def main():
    parser = argparse.ArgumentParser(
        description="generate a job spec file for the RdRand application")
    parser.add_argument("input_directory", help="the directory containing the "
                        "binary sequences of 64-bit fragments")
    parser.add_argument("--output_directory", help="the directory that will "
                        "contain the duplicate key information (default: "
                        "<username>/outputs)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="rdrand.json")
    parser.add_argument("-u", "--username", help="the username on whose data "
                        "the job is being run", default=getpass.getuser())

    args = parser.parse_args()

    config = rdrand(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
