#!/usr/bin/env python

import os, sys, argparse
from operator import itemgetter

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

from themis.metaprograms.utils import StatQuery
import themis.metaprograms.utils as utils

def get_list_time_series_queries():

    worker_query = StatQuery(
        "COLL",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("collection_stat_name", None))

    def worker_match_function(match, data):
        phase = match["phase_name"]
        stage = match["stage_name"]
        worker_id = str(match["id"])
        stat_name = match["collection_stat_name"]

        if "time_series_keys" not in data:
            data["time_series_keys"] = set()

        time_series_key = (phase, stage, worker_id, stat_name)
        data["time_series_keys"].add(time_series_key)

    worker_query.match_processor_function = worker_match_function

    tracker_query = StatQuery(
        "COLL",
        ("phase_name", None),
        ("epoch", None),
        ("logger_name", None),
        ("collection_stat_name", None))

    def tracker_match_function(match, data):
        phase = match["phase_name"]
        stage = match["logger_name"]
        stat_name = match["collection_stat_name"]

        if "time_series_keys" not in data:
            data["time_series_keys"] = set()

        time_series_key = (phase, stage, None, stat_name)
        data["time_series_keys"].add(time_series_key)

    tracker_query.match_processor_function = tracker_match_function

    return (worker_query, tracker_query)

def time_series_tuple_to_str(time_series_tuple):
    if time_series_tuple[2] == None:
        return "%s.%s" % (".".join(time_series_tuple[0:2]), ".".join(
                time_series_tuple[3:]))
    else:
        return ".".join(time_series_tuple)

def list_time_series(
    experiment_log_dir, plot_spec_strings, output_filename, verbose):
    queries = []

    queries.extend(get_list_time_series_queries())
    time_series_data = utils.process_queries(
        queries, experiment_log_dir, verbose)
    time_series_keys = time_series_data["time_series_keys"]

    output_fp = open(output_filename, 'w')
    for time_series_key in sorted(time_series_keys, key=itemgetter(0,1,3,2)):
        print >> output_fp, time_series_tuple_to_str(time_series_key)

    output_fp.close()

def main():
    parser = argparse.ArgumentParser(
        description="List available time-series data from a log directory")
    parser.add_argument("experiment_log_dir", help="a directory containing log "
                        "data for an experiment")
    parser.add_argument(
        "--output", "-o", help="output filename (default: %(default)s)",
        default="time_series_keys.txt")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    list_time_series(experiment_log_dir = args.experiment_log_dir,
                     plot_spec_strings = ["*.*.*.*.*"],
                     output_filename = args.output, verbose=args.verbose)

if __name__ == "__main__":
    main()
