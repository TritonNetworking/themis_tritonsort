#!/usr/bin/env python

import sys, argparse, json, utils, os

sys.path.append(
    os.path.join(
        os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))

import common.unitconversion as uc


def disk_benchmark(
    input_directory, output_directory, benchmark_size_per_disk, **kwargs):

    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "disk_speeds")


    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory, None)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "DiskBenchmarkMapFunction",
        reduce_function = "DiskBenchmarkReduceFunction")

    utils.force_single_partition(config)

    data_size_bytes = int(uc.parse_and_convert(benchmark_size_per_disk, "B"))

    config_params = {
        "DISK_BENCHMARK_DATA_SIZE" : data_size_bytes
        }

    if "params" not in config:
        config["params"] = {}

    for key, value in config_params.items():
        config["params"][key] = value

    return config

def main():
    parser = argparse.ArgumentParser(
        description="benchmark each disk's write speed")

    parser.add_argument(
        "input_directory", help="input directory containing records generated "
        "by generate_disk_path_tuples.py")
    parser.add_argument(
        "benchmark_size_per_disk", help="the amount of data to write per disk "
        "during the benchmark")
    parser.add_argument(
        "--output_directory", help="the directory that will contain the "
        "measured speeds (default: a sibling of the input directory named "
        "'disk_speeds')")
    parser.add_argument(
        "-o", "--output_filename", help="name of the job spec file to write "
        "(default: %(default)s)", default="disk_benchmark.json")

    args = parser.parse_args()
    config = disk_benchmark(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
