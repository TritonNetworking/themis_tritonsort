#!/usr/bin/env python

import sys, argparse, utils, json
from merge_files import merge_files

def cloudburst(
    input_directory, output_directory, hdfs, min_read_len, max_read_len,
    max_align_diff, redundancy, allow_differences, block_size, **kwargs):

    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_cloudburst_aligned")

    intermediate_directory = utils.sibling_directory(
        input_directory, "%(dirname)s_cloudburst_unmerged")

    (input_url, intermediate_url) = utils.generate_urls(
        input_directory, intermediate_directory, hdfs)

    cloudburst_config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = intermediate_url,
        map_function = "CloudBurstMapFunction",
        reduce_function = "CloudBurstReduceFunction",
        partition_function = "CloudBurstPartitionFunction")

    seed_len = min_read_len / (max_align_diff +1)
    flank_len = max_read_len - seed_len + max_align_diff

    cloudburst_params = {
        "CLOUDBURST_MIN_READ_LEN" : min_read_len,
        "CLOUDBURST_MAX_READ_LEN" : max_read_len,
        "CLOUDBURST_MAX_ALIGN_DIFF" : max_align_diff,
        "CLOUDBURST_SEED_LEN" : seed_len,
        "CLOUDBURST_FLANK_LEN" : flank_len,
        "CLOUDBURST_REDUNDANCY" : redundancy,
        "CLOUDBURST_ALLOW_DIFFERENCES" : int(allow_differences),
        "CLOUDBURST_BLOCK_SIZE" : block_size
        }

    if "params" not in cloudburst_config:
        cloudburst_config["params"] = {}

    for key, value in cloudburst_params.items():
        cloudburst_config["params"][key] = value

    mergefiles_config = merge_files(
        intermediate_directory, output_directory, hdfs)

    return utils.run_in_sequence(cloudburst_config, mergefiles_config)

def main():
    parser = argparse.ArgumentParser(
        description="run CloudBurst short-read sequence alignment on a genome")
    parser.add_argument(
        "input_directory", help="the directory containing the documents "
        "whose words should be counted")
    parser.add_argument(
        "--output_directory", help="the directory that will contain the "
        "results of this job (default: a sibling of the input directory named "
        "'<input directory's name>_cloudburst_aligned')")
    parser.add_argument(
        "-o", "--output_filename", help="name of the job spec file to write "
        "(default: %(default)s)", default="cloudburst.json")
    parser.add_argument(
        "--hdfs", help="host:port specifying the HDFS namenode where input and "
        "output data should be stored (default: store data on local disks "
        "rather than on HDFS)")
    parser.add_argument(
        "--min_read_len", type=int, help="minimum read length of reads "
        "(default: %(default)s)", default=36)
    parser.add_argument(
        "--max_read_len", type=int, help="maximum read length of reads "
        "(default: %(default)s)", default=36)
    parser.add_argument(
        "--max_align_diff", type=int, help="number of differences to allow "
        "(higher number requires more time) (default: %(default)s)", default=3)
    parser.add_argument(
        "--redundancy",type=int, help="number of copies of low complexity"
        "seeds to use. (suggested: # reducers) (default: %(default)s)",
        default=16)
    parser.add_argument(
        "--allow_differences", help="allow indels as well as mismatches",
        default=False, action="store_true")
    parser.add_argument(
        "--block_size", type=int,
        help="number of query and reference tuples to consider at a time in "
        "the reduce phase. (default: %(default)s)", default=128)


    args = parser.parse_args()
    config = cloudburst(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
