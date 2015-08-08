#!/usr/bin/env python

import os, sys, argparse, jinja2, math, itertools

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

from utils import StatContainer, StatQuery
import utils

"""
Various formulae extracted from Wikipedia
"""

def sample_mean(samples):
    return float(sum(samples)) / float(len(samples))

def sample_moment(samples, moment_number):
    mean = sample_mean(samples)

    return sum(itertools.imap(
            lambda x: math.pow(x - mean, moment_number),
            samples)) / len(samples)

def sample_skewness(samples):
    return sample_moment(samples, 3) / math.pow(sample_moment(samples, 2), 1.5)

def sample_excess_kurtosis(samples):
    return (sample_moment(samples, 4) /
            math.pow(sample_moment(samples, 2), 2)) - 3

def population_variance(samples):
    return sample_moment(samples, 2)


def handle_write_size_match(match, data):
    job = match["job_name"]
    phase = match["phase_name"]

    data_key = (job, phase)

    if "write_sizes" not in data:
        data["write_sizes"] = {}

    if data_key not in data["write_sizes"]:
        data["write_sizes"][data_key] = []

    data["write_sizes"][data_key].append(match["value"])

def write_size_statistics(input_directory, verbose):
    write_sizes_query = StatQuery(
        "COLL",
        ("phase_name", "phase_one"),
        ("epoch", 0),
        ("stage_name", "writer"),
        ("id", None),
        ("collection_stat_name", "write_size"))
    write_sizes_query.match_processor_function = handle_write_size_match

    queries = [write_sizes_query]

    data = utils.process_queries(queries, input_directory, verbose)

    write_sizes = data["write_sizes"]

    env = jinja2.Environment(loader = jinja2.FileSystemLoader(SCRIPT_DIR),
                             trim_blocks=True)
    template = env.get_template("write_size_stats_template.jinja2")

    for phase_key in data["write_sizes"]:
        job, phase = phase_key

        current_write_sizes = data["write_sizes"][phase_key]
        current_write_sizes.sort()

        template_args = {
            "job" : job,
            "phase" : phase,
            "min" : min(current_write_sizes),
            "max" : max(current_write_sizes),
            "median" : current_write_sizes[len(current_write_sizes) / 2],
            "mean" : sample_mean(current_write_sizes),
            "stddev" : math.sqrt(population_variance(current_write_sizes)),
            "skewness" : sample_skewness(current_write_sizes),
            "excess_kurtosis" : sample_excess_kurtosis(current_write_sizes)
            }

        print template.render(**template_args)

def main():
    parser = argparse.ArgumentParser(
        description="extract statistics about write sizes for phase one of a "
        "MapReduce job")

    parser.add_argument("input_directory", help="log directory to analyze")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")
    args = parser.parse_args()

    for dirname in [args.input_directory]:
        if not os.path.exists(dirname) or not os.path.isdir(dirname):
            parser.error("'%s' doesn't exist or is not a directory" %
                         (dirname))
    write_size_statistics(**vars(args))

if __name__ == "__main__":
    main()
