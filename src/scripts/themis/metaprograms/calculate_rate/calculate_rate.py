#!/usr/bin/env python

import os, sys, argparse, pprint

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

from themis.metaprograms.utils import StatContainer, StatQuery
import themis.metaprograms.utils as utils
from descriptions.descriptionutils import Description
from common.unitconversion import convert
import jinja2

def handleTimestampQueryMatch(match, data):
    job = match["job_name"]
    phase = match["phase_name"]
    epoch = match["epoch"]
    start_time = match["start_time"]
    stop_time = match["stop_time"]

    start_time_subdict = utils.populate_nested_dictionary(
        data,
        key_list=[(job, phase, epoch), "stats", "timestamps", "start"],
        types=[dict, dict, dict, StatContainer])
    start_time_subdict.append(start_time)

    stop_time_subdict = utils.populate_nested_dictionary(
        data,
        key_list=[(job, phase, epoch), "stats", "timestamps", "stop"],
        types=[dict, dict, dict, StatContainer])
    stop_time_subdict.append(stop_time)

def handleDiskCountMatch(match, data):
    job = match["job_name"]
    stat_name = match["stat_name"]
    hostname = match["hostname"]

    disks_subdict = utils.populate_nested_dictionary(
        data, [(job, None, None), "disks", hostname])
    disks_subdict[stat_name] = match["uint_value"]

def handleReaderInputMatch(match, data):
    job = match["job_name"]
    phase_name = match["phase_name"]
    epoch = match["epoch"]
    value = match["uint_value"]
    hostname = match["hostname"]

    hosts_subdict = utils.populate_nested_dictionary(
        data,
        key_list=[(job, phase_name, epoch), "hosts"],
        types=[dict, set])
    hosts_subdict.add(hostname)

    data_subdict = utils.populate_nested_dictionary(
        data, [(job, phase_name, epoch), "stats"])

    if "input_size" not in data_subdict:
        data_subdict["input_size"] = 0

    data_subdict["input_size"] += value

def handleWriterOutputMatch(match, data):
    job = match["job_name"]
    phase_name = match["phase_name"]
    epoch = match["epoch"]
    value = match["uint_value"]

    data_subdict = utils.populate_nested_dictionary(
        data, [(job, phase_name, epoch), "stats"])

    if "output_size" not in data_subdict:
        data_subdict["output_size"] = 0

    data_subdict["output_size"] += value

def postprocess_rate_data(data):
    display_data = {}

    disk_data = {}

    for data_key in sorted(data.keys()):
        (job, phase, epoch) = data_key

        epoch_data = data[data_key]

        if phase is None and epoch is None:
            disk_data = epoch_data["disks"]

            for key, value in disk_data.items():
                disk_data[key] = value

            continue

        epoch_display_data = {}

        epoch_display_data["job"] = job
        epoch_display_data["phase"] = phase
        epoch_display_data["epoch"] = epoch

        host_data = epoch_data["hosts"]
        stats_data = epoch_data["stats"]

        epoch_display_data["intermediate_disks"] = sum(
            (disk_data[host]["num_intermediate_disks"] for host in disk_data))

        epoch_display_data["num_nodes"] = len(host_data)

        input_size = stats_data["input_size"]
        if "output_size" in stats_data:
            output_size = stats_data["output_size"]
        else:
            output_size = 0
        max_size = max(input_size, output_size)

        for (size_name, size) in [("input_size", input_size),
                                  ("output_size", output_size),
                                  ("max_size", max_size)]:

            epoch_display_data[size_name] = {}

            for unit in ["B", "MB", "MiB", "GB", "Gb", "GiB", "TB"]:
                epoch_display_data[size_name][unit] = convert(
                    size, "B", unit)

        epoch_display_data["total_time"] = {}

        if "timestamps" in stats_data:
            smallest_start_time = stats_data["timestamps"]["start"].min()
            largest_stop_time = stats_data["timestamps"]["stop"].max()

            total_time = largest_stop_time - smallest_start_time

            for unit in ["s", "min"]:
                epoch_display_data["total_time"][unit] = convert(
                    total_time, "us", unit)
        else:
            for unit in ["s", "min"]:
                epoch_display_data["total_time"][unit] = 0


        display_data[data_key] = epoch_display_data

    return display_data

def calculate_rate(
    input_directory, skip_phase_zero, skip_phase_one, skip_phase_two, verbose):

    phaseTimesQuery = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("logger_name", None),
        ("stat_name", "phase_runtime"),
        ("start_time", None))
    phaseTimesQuery.match_processor_function = handleTimestampQueryMatch

    diskCountQuery = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("logger_name", "mapreduce"),
        ("stat_name", ["num_input_disks", "num_intermediate_disks"]),
        ("uint_value", None))
    diskCountQuery.match_processor_function = handleDiskCountMatch

    inputSizeQuery = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", "reader"),
        ("id", None),
        ("stat_name", "bytes_produced"),
        ("uint_value", None)
        )
    inputSizeQuery.match_processor_function = handleReaderInputMatch

    writerOutputQuery = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", "writer"),
        ("id", None),
        ("stat_name", "bytes_consumed"),
        ("uint_value", None)
        )
    writerOutputQuery.match_processor_function = handleWriterOutputMatch

    queries = [phaseTimesQuery, diskCountQuery, inputSizeQuery,
               writerOutputQuery]

    skipped_phases = []
    if skip_phase_zero:
        skipped_phases.append("phase_zero")
    if skip_phase_one:
        skipped_phases.append("phase_one")
    if skip_phase_two:
        skipped_phases.append("phase_two")
    output_data = utils.process_queries(
        queries, input_directory, verbose, skipped_phases)

    data_for_display = postprocess_rate_data(output_data)

    for key in sorted(data_for_display.keys()):
        env = jinja2.Environment(
            loader = jinja2.FileSystemLoader(os.path.dirname(__file__)),
            trim_blocks = True)

        template = env.get_template('rate_summary_template.jinja')

        rendered_template = template.render(**data_for_display[key])

        print rendered_template.strip() + "\n"


def main():
    parser = argparse.ArgumentParser(
        description = "calculate data processing rate of the target "
        "application")

    parser.add_argument("input_directory", help="log directory to analyze")
    parser.add_argument(
        "--skip_phase_zero", help="don't process phase zero", default=False,
        action="store_true")
    parser.add_argument(
        "--skip_phase_one", help="don't process phase one", default=False,
        action="store_true")
    parser.add_argument(
        "--skip_phase_two", help="don't process phase two", default=False,
        action="store_true")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    if args.input_directory[:-1] != '/':
        args.input_directory += '/'

    for dirname in [args.input_directory]:
        if not os.path.exists(dirname) or not os.path.isdir(dirname):
            parser.error("'%s' doesn't exist or is not a directory" %
                         (dirname))

    calculate_rate(**vars(args))

if __name__ == "__main__":
    main()
